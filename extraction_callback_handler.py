"""
Extraction Callback Lambda (idp-extraction-callback).
SNS-triggered on Textract job completion.
 
Phase 2C-revised: keeps Geometry in saved OCR JSON so the UI can use
bounding boxes for the Click N and Search features.
"""
import json
import logging
import os
import re
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple
import boto3
from shared import config_loader, ddb
logger = logging.getLogger()
logger.setLevel(logging.INFO)
_s3 = boto3.client("s3")
_textract = boto3.client("textract")
_sfn = boto3.client("stepfunctions")
FIELD_VALUE_WINDOW = 120
 
class _DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            if o == o.to_integral_value():
                return int(o)
            return float(o)
        return super().default(o)
 
def _j(obj: Any) -> str:
    return json.dumps(obj, cls=_DecimalEncoder)
 
def _fetch_full_textract_result(job_id: str) -> Dict[str, Any]:
    all_blocks: List[Dict[str, Any]] = []
    document_metadata: Optional[Dict[str, Any]] = None
    next_token = None
    while True:
        kwargs = {"JobId": job_id}
        if next_token:
            kwargs["NextToken"] = next_token
        resp = _textract.get_document_analysis(**kwargs)
        if document_metadata is None:
            document_metadata = resp.get("DocumentMetadata")
        all_blocks.extend(resp.get("Blocks", []))
        next_token = resp.get("NextToken")
        if not next_token:
            break
    return {
        "DocumentMetadata": document_metadata,
        "JobStatus": resp.get("JobStatus"),
        "Blocks": all_blocks,
    }
 
def _summarize_blocks(blocks: List[Dict[str, Any]]) -> Tuple[str, List[Dict[str, Any]], int]:
    lines: List[Dict[str, Any]] = []
    word_count = 0
    for b in blocks:
        bt = b.get("BlockType")
        if bt == "LINE":
            lines.append({
                "text": b.get("Text", ""),
                "confidence": float(b.get("Confidence", 0.0)),
            })
        elif bt == "WORD":
            word_count += 1
    joined = "\n".join(line["text"] for line in lines)
    return joined, lines, word_count
 
def _slim_blocks(blocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Slim each block for S3 storage. Keeps Geometry so the UI can use
    bounding boxes for Click N and Search. The 'Polygon' attribute inside
    Geometry is dropped because we only need 'BoundingBox' on the client.
    """
    out = []
    for b in blocks:
        geom_in = b.get("Geometry") or {}
        bbox = geom_in.get("BoundingBox")
        slim_geom = {"BoundingBox": bbox} if bbox else None
        out.append({
            "BlockType": b.get("BlockType"),
            "Id": b.get("Id"),
            "Page": b.get("Page"),
            "Text": b.get("Text"),
            "Confidence": b.get("Confidence"),
            "EntityTypes": b.get("EntityTypes"),
            "Relationships": b.get("Relationships"),
            "Geometry": slim_geom,
        })
    return out
 
def _extract_form_kvs(blocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_id = {b["Id"]: b for b in blocks if b.get("Id")}
    def _get_text_for_block(block):
        rels = block.get("Relationships") or []
        word_texts = []
        confidences = []
        for r in rels:
            if r.get("Type") != "CHILD":
                continue
            for child_id in r.get("Ids", []) or []:
                child = by_id.get(child_id)
                if not child:
                    continue
                if child.get("BlockType") == "WORD":
                    word_texts.append(child.get("Text", ""))
                    confidences.append(float(child.get("Confidence", 0.0)))
                elif child.get("BlockType") == "SELECTION_ELEMENT":
                    word_texts.append("X" if child.get("SelectionStatus") == "SELECTED" else "")
        text = " ".join(t for t in word_texts if t).strip()
        conf = sum(confidences) / len(confidences) if confidences else float(block.get("Confidence", 0.0))
        return text, conf
    out = []
    for b in blocks:
        if b.get("BlockType") != "KEY_VALUE_SET":
            continue
        if "KEY" not in (b.get("EntityTypes") or []):
            continue
        key_text, key_conf = _get_text_for_block(b)
        value_text = ""
        value_conf = 0.0
        for r in b.get("Relationships") or []:
            if r.get("Type") != "VALUE":
                continue
            for value_id in r.get("Ids", []) or []:
                vb = by_id.get(value_id)
                if vb:
                    value_text, value_conf = _get_text_for_block(vb)
        if key_text:
            out.append({
                "key_text": key_text,
                "key_confidence": round(key_conf, 2),
                "value_text": value_text,
                "value_confidence": round(value_conf, 2),
            })
    return out
 
def _safe_compile(pattern, field_name):
    if not pattern:
        return None
    try:
        return re.compile(pattern)
    except re.error:
        logger.warning("Bad ExtractionRegex for field %s: %r", field_name, pattern)
        return None
 
def _match_field_via_kvs(field_cfg, keywords, kv_pairs):
    regex = _safe_compile(field_cfg.get("ExtractionRegex"), field_cfg.get("FieldName"))
    for kw in keywords:
        kw_lower = kw.lower()
        for kv in kv_pairs:
            if kw_lower not in (kv.get("key_text") or "").lower():
                continue
            value_text = (kv.get("value_text") or "").strip()
            if not value_text:
                continue
            if regex:
                m = regex.search(value_text)
                if not m:
                    continue
                value = m.group(0).strip()
            else:
                value = value_text
            if not value:
                continue
            return {
                "value": value,
                "confidence": kv.get("value_confidence", 0.0),
                "matchedKeyword": kw,
                "source": "FORMS",
            }
    return None
 
def _confidence_for_value(value, lines):
    confidences = [line["confidence"] for line in lines if value and value.lower() in line["text"].lower()]
    if confidences:
        return round(sum(confidences) / len(confidences), 2)
    if lines:
        return round(max(line["confidence"] for line in lines), 2)
    return 0.0
 
def _match_field_via_text(field_cfg, keywords, text, lines):
    regex = _safe_compile(field_cfg.get("ExtractionRegex"), field_cfg.get("FieldName"))
    if not regex:
        return None
    text_lower = text.lower()
    for kw in keywords:
        idx = text_lower.find(kw.lower())
        if idx < 0:
            continue
        window = text[idx + len(kw):idx + len(kw) + FIELD_VALUE_WINDOW]
        m = regex.search(window)
        if not m:
            continue
        value = m.group(0).strip()
        if not value:
            continue
        return {
            "value": value,
            "confidence": _confidence_for_value(value, lines),
            "matchedKeyword": kw,
            "source": "TEXT",
        }
    return None
 
def _extract_fields_for_page(blocks, main_page_cfg):
    fields = main_page_cfg.get("Fields", []) or []
    if not fields:
        return {}
    kv_pairs = _extract_form_kvs(blocks)
    text, lines, _ = _summarize_blocks(blocks)
    results = {}
    for f in fields:
        name = f["FieldName"]
        keywords = [k for k in (f.get("FieldKeywords") or []) if k]
        if not keywords:
            continue
        kv_match = _match_field_via_kvs(f, keywords, kv_pairs)
        if kv_match:
            results[name] = kv_match
            continue
        text_match = _match_field_via_text(f, keywords, text, lines)
        if text_match:
            results[name] = text_match
    return results
 
def _merge_into_document_fields(doc_fields, page_id, page_extractions):
    out = []
    for fld in doc_fields:
        merged = dict(fld)
        name = merged.get("fieldName")
        already_has_value = merged.get("value") not in (None, "", [])
        if name in page_extractions and not already_has_value:
            res = page_extractions[name]
            merged["value"] = res["value"]
            merged["confidence"] = res["confidence"]
            merged["sourcePageId"] = page_id
        out.append(merged)
    return out
 
def _save_ocr_to_s3(bucket, application, queue_id, page_id, payload):
    key = f"idp/applications/{application}/Processing/{queue_id}/ocr/{page_id}.json"
    _s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(payload).encode("utf-8"),
                   ContentType="application/json")
    return key
 
def _read_ocr_from_s3(bucket, key):
    resp = _s3.get_object(Bucket=bucket, Key=key)
    return json.loads(resp["Body"].read().decode("utf-8"))
 
def _reclassify(original_page_type, word_count, text_lower, blank_max, cover_keywords):
    if word_count < blank_max:
        return "BlankPage"
    if cover_keywords and any(kw.lower() in text_lower for kw in cover_keywords if kw):
        return "CoverPage"
    return original_page_type
 
def _get_page_type_config(doc_type, page_type_name):
    for pt in doc_type.get("PageTypes", []):
        if pt.get("PageType") == page_type_name:
            return pt
    return None
 
def _get_cover_keywords(doc_type):
    cover = _get_page_type_config(doc_type, "CoverPage")
    return (cover or {}).get("Keywords", []) or []

def _get_blank_page_max_word_count(doc_type, app_cfg):
    """Studio moved this from Application-level (`BlankPageMaxWordCount`)
    to Page-Type-level (`MaxWordCount`, on whichever page type uses
    `IdentificationMethod: 'WordCount'` -- typically BlankPage) as a
    deliberate breaking change (see Studio's README). This Lambda was
    still reading the old Application field, which Studio no longer
    writes -- meaning every Document Type saved/edited in Studio silently
    reverted blank-page detection to the hardcoded default of 5.

    Reads the new per-Page-Type field first; falls back to the legacy
    Application-level field for any Application that predates the Studio
    migration and hasn't been re-saved since (the migration script warns
    but doesn't drop this field, per Studio's README); falls back to 5
    only if neither is present."""
    for pt in doc_type.get("PageTypes", []):
        if pt.get("IdentificationMethod") == "WordCount" and pt.get("MaxWordCount") is not None:
            return int(pt["MaxWordCount"])
    if app_cfg.get("BlankPageMaxWordCount") is not None:
        return int(app_cfg["BlankPageMaxWordCount"])
    return 5
 
def _get_application_for_queue(queue_id):
    meta = ddb.get_batch(queue_id)
    if not meta:
        raise RuntimeError(f"No batch META for queueId={queue_id}")
    return meta["application"]
 
def lambda_handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    logger.info("Event: %s", _j(event))
    docs_bucket = os.environ["DOCS_BUCKET"]
    for record in event.get("Records", []):
        msg = json.loads(record.get("Sns", {}).get("Message", "{}"))
        _process_one_callback(msg, docs_bucket)
    return {"ok": True}
 
def _process_one_callback(msg, docs_bucket):
    job_id = msg.get("JobId")
    status = msg.get("Status")
    logger.info("Callback for JobId=%s status=%s", job_id, status)
    job_row = ddb.get_textract_job_by_id(job_id)
    if not job_row:
        logger.error("No TEXTRACTJOB row for jobId=%s; ignoring", job_id)
        return
    queue_id = int(job_row["queueId"])
    page_id = job_row["pageId"]
    task_token = job_row["taskToken"]
    if status != "SUCCEEDED":
        logger.error("Textract job %s ended in status=%s", job_id, status)
        ddb.update_textract_job_status(
            queue_id=queue_id, job_id=job_id, status="Failed",
            completed_at=ddb.now_iso(),
            error_message=msg.get("StatusMessage") or status,
        )
        try:
            _sfn.send_task_failure(
                taskToken=task_token,
                error="TextractJobFailed",
                cause=f"JobId={job_id} status={status}",
            )
        except Exception:
            logger.exception("send_task_failure failed (may already be resolved)")
        return
    full = _fetch_full_textract_result(job_id)
    blocks = full.get("Blocks", [])
    text, lines, word_count = _summarize_blocks(blocks)
    page = ddb.get_page(queue_id, page_id)
    if not page:
        logger.error("No PAGE row for queueId=%s pageId=%s", queue_id, page_id)
        return
    application = _get_application_for_queue(queue_id)
    ocr_key = _save_ocr_to_s3(
        bucket=docs_bucket, application=application,
        queue_id=queue_id, page_id=page_id,
        payload={"DocumentMetadata": full.get("DocumentMetadata"),
                 "Blocks": _slim_blocks(blocks)},
    )
    app_cfg = config_loader.get_application(application)
    doc_type = app_cfg["DocumentTypes"][0]
    cover_kw = _get_cover_keywords(doc_type)
    blank_max = _get_blank_page_max_word_count(doc_type, app_cfg)
    original_type = page.get("pageType", "Unclassified")
    new_type = _reclassify(original_type, word_count, text.lower(), blank_max, cover_kw)
    ddb.update_page_after_ocr(
        queue_id=queue_id, page_id=page_id,
        ocr_text_s3_key=ocr_key, word_count=word_count,
        page_type=new_type if new_type != original_type else None,
        page_status="Extracted",
    )
    ddb.update_textract_job_status(
        queue_id=queue_id, job_id=job_id, status="Completed",
        completed_at=ddb.now_iso(), result_s3_key=ocr_key,
    )
    new_count = ddb.decrement_pending_pages_count(queue_id)
    logger.info("pendingPagesCount after decrement: %d", new_count)
    if new_count > 0:
        return
    logger.info("Last page complete. Final extraction for queueId=%d", queue_id)
    _run_final_extraction(queue_id, application, docs_bucket, task_token)
 
def _run_final_extraction(queue_id, application, docs_bucket, task_token):
    start_time = ddb.now_iso()
    try:
        app_cfg = config_loader.get_application(application)
        doc_type = app_cfg["DocumentTypes"][0]
        main_page_cfg = _get_page_type_config(doc_type, "MainPage")
        document = ddb.get_first_document(queue_id)
        if not document:
            raise RuntimeError(f"No document for queueId={queue_id}")
        pages = ddb.list_pages(queue_id)
        merged_fields = document.get("extractedFields") or []
        per_page_summary = []
        for page in pages:
            page_id = page["pageId"]
            ocr_key = page.get("ocrTextS3Key")
            page_type = page.get("pageType", "Unclassified")
            if page_type != "MainPage":
                per_page_summary.append({"pageId": page_id, "pageType": page_type, "fieldsFound": []})
                continue
            if not ocr_key:
                per_page_summary.append({"pageId": page_id, "skipped": "no-ocr"})
                continue
            ocr_data = _read_ocr_from_s3(docs_bucket, ocr_key)
            blocks = ocr_data.get("Blocks", [])
            page_extractions = _extract_fields_for_page(blocks, main_page_cfg or {})
            merged_fields = _merge_into_document_fields(merged_fields, page_id, page_extractions)
            per_page_summary.append({
                "pageId": page_id, "pageType": page_type,
                "fieldsFound": list(page_extractions.keys()),
            })
        ddb.update_document_fields(
            queue_id=queue_id,
            doc_id=document["documentId"],
            extracted_fields=merged_fields,
        )
        ddb.append_task_history(
            queue_id=queue_id, task_name="Extraction", status="Completed",
            start_time=start_time, end_time=ddb.now_iso(), operator="system",
            details={
                "pages": per_page_summary,
                "fieldsPopulated": sum(1 for f in merged_fields if f.get("value") not in (None, "")),
            },
        )
        _sfn.send_task_success(
            taskToken=task_token,
            output=json.dumps({
                "queueId": queue_id,
                "application": application,
                "pagesExtracted": len(pages),
                "fieldsPopulated": sum(1 for f in merged_fields if f.get("value") not in (None, "")),
            }),
        )
        logger.info("Sent SendTaskSuccess for queueId=%d", queue_id)
    except Exception as e:
        logger.exception("Final extraction failed")
        ddb.append_task_history(
            queue_id=queue_id, task_name="Extraction", status="Failed",
            start_time=start_time, end_time=ddb.now_iso(), operator="system",
        )
        try:
            _sfn.send_task_failure(
                taskToken=task_token,
                error="FinalExtractionFailed",
                cause=str(e)[:256],
            )
        except Exception:
            logger.exception("send_task_failure failed")
        raise