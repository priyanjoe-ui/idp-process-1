"""
Test Bench Lambda (idp-test-bench).

Synchronous, on-demand classification + extraction against a single
uploaded sample -- deliberately NOT the real pipeline. Invoked directly
(RequestResponse) by idp-api-proxy: no SQS, no S3 trigger, no Step
Functions execution, no DynamoDB batch-tracking rows written. Nothing
about running a test here is visible in production monitoring, and
nothing it does can affect a real batch.

Reuses the EXACT SAME classification/extraction logic the real pipeline
uses -- shared.classification_logic / shared.extraction_logic -- so a
result here is guaranteed consistent with what a real batch would
produce. The only things genuinely different are:
  - Pages come from an uploaded file already in memory, not an
    already-ingested S3 object.
  - Textract is called synchronously (AnalyzeDocument), once per page,
    instead of the real pipeline's async job + SNS callback pattern --
    that pattern exists to handle scale across many concurrent batches,
    which a single ad-hoc test sample never needs.

v1 input formats: .tif/.tiff (single- or multi-page) and .zip (containing
.tif/.tiff files, naturally sorted -- mirroring idp-ingestion's own
convention for a real batch's pages). No PDF support yet.
"""
import base64
import io
import json
import logging
import re
import zipfile
from typing import Any, Dict, List

import boto3
from PIL import Image

from shared import classification_logic, config_loader, extraction_logic

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_textract = boto3.client("textract")

_NUM_RE = re.compile(r"(\d+)")


def _natural_key(name: str) -> List[Any]:
    """Same natural-sort convention idp-ingestion already uses for a real
    batch's page ordering, so a .zip tested here sorts identically to how
    it would if it went through real ingestion."""
    return [int(p) if p.isdigit() else p.lower() for p in _NUM_RE.split(name)]


def _split_input_into_pages(file_bytes: bytes, file_name: str) -> List[bytes]:
    """
    Returns one page image per page (single-frame TIFF bytes), in reading
    order. Textract's synchronous AnalyzeDocument/DetectDocumentText accept
    TIFF directly, so each split-out frame is re-saved as its own
    standalone single-page TIFF, in its original color mode -- no format
    conversion, no colorspace change.
      .zip           -- every .tif/.tiff inside, naturally sorted.
      .tif / .tiff    -- every frame of the (possibly multi-page) TIFF.
    Raises ValueError with a message safe to show directly in Studio's UI
    for any unsupported/empty input.
    """
    lower = file_name.lower()
    tiff_blobs: List[bytes] = []

    if lower.endswith(".zip"):
        with zipfile.ZipFile(io.BytesIO(file_bytes)) as zf:
            names = [
                n for n in zf.namelist()
                if n.lower().endswith((".tif", ".tiff")) and not n.endswith("/")
            ]
            names.sort(key=_natural_key)
            if not names:
                raise ValueError("No .tif/.tiff files found inside the uploaded .zip")
            for n in names:
                tiff_blobs.append(zf.read(n))
    elif lower.endswith((".tif", ".tiff")):
        tiff_blobs.append(file_bytes)
    else:
        raise ValueError(
            f"Unsupported file type for '{file_name}' -- Test Bench accepts .tif, .tiff, or .zip"
        )

    pages: List[bytes] = []
    for blob in tiff_blobs:
        img = Image.open(io.BytesIO(blob))
        frame_idx = 0
        while True:
            try:
                img.seek(frame_idx)
            except EOFError:
                break
            out = io.BytesIO()
            img.save(out, format="TIFF")
            pages.append(out.getvalue())
            frame_idx += 1

    if not pages:
        raise ValueError("No page images found in the uploaded file")
    return pages


def _find_document_type(app_cfg: Dict[str, Any], document_type_id: str) -> Dict[str, Any]:
    for dt in app_cfg.get("DocumentTypes", []):
        if dt.get("DocumentTypeName") == document_type_id:
            return dt
    raise ValueError(
        f"Document Type '{document_type_id}' not found in Application "
        f"'{app_cfg.get('ApplicationName')}'"
    )


def _get_page_type_config(doc_type: Dict[str, Any], page_type_name: str) -> Dict[str, Any]:
    for pt in doc_type.get("PageTypes", []):
        if pt.get("PageType") == page_type_name:
            return pt
    return {}


_PREVIEW_MAX_DIMENSION = 1400
_PREVIEW_JPEG_QUALITY = 80


def _make_preview_image(page_bytes: bytes) -> str:
    """
    Base64 JPEG preview for Studio's document viewer -- a completely
    separate copy from the TIFF bytes sent to Textract, so this never
    affects analysis accuracy. TIFF doesn't render in any browser (no
    <img> tag supports it), which is the whole reason this exists.
    Resized to a max dimension so a multi-page response stays well under
    the Lambda payload ceiling -- this is a display copy, not something
    anyone needs full scan resolution for.
    """
    img = Image.open(io.BytesIO(page_bytes)).convert("RGB")
    if max(img.size) > _PREVIEW_MAX_DIMENSION:
        img.thumbnail((_PREVIEW_MAX_DIMENSION, _PREVIEW_MAX_DIMENSION), Image.LANCZOS)
    out = io.BytesIO()
    img.save(out, format="JPEG", quality=_PREVIEW_JPEG_QUALITY)
    return base64.b64encode(out.getvalue()).decode("ascii")


def lambda_handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    """
    Expected event shape (relayed as-is from idp-api-proxy's
    POST /api/test-bench/run):
      {
        "applicationId": "AFLCOR",
        "documentTypeId": "CORRDOC",
        "fileName": "policy-001.zip",
        "fileBase64": "<base64-encoded file bytes>"
      }
    """
    application_id = event["applicationId"]
    document_type_id = event["documentTypeId"]
    file_name = event["fileName"]
    file_bytes = base64.b64decode(event["fileBase64"])

    logger.info("Test Bench run: app=%s docType=%s file=%s (%d bytes)",
                application_id, document_type_id, file_name, len(file_bytes))

    app_cfg = config_loader.get_application(application_id)
    doc_type = _find_document_type(app_cfg, document_type_id)
    mode = app_cfg.get("ClassificationMode", "Sequential")
    page_types_cfg = doc_type.get("PageTypes", [])

    page_images = _split_input_into_pages(file_bytes, file_name)
    logger.info("Split into %d page(s)", len(page_images))

    page_results = []
    for idx, page_bytes in enumerate(page_images, start=1):
        # --- Classification -----------------------------------------
        # Same two modes, same decision rule the real pipeline uses --
        # via shared.classification_logic, not a reimplementation.
        if mode == "OCR":
            words = classification_logic.ocr_words(image_bytes=page_bytes)
            page_type_name = classification_logic.classify_ocr_words(words, page_types_cfg)
        else:
            if mode not in ("Sequential",):
                logger.warning("Unknown ClassificationMode %r, falling back to Sequential", mode)
            page_type_name = "MainPage" if idx == 1 else "TrailingPage"

        # --- Extraction (only if this Page Type has Fields at all) --
        page_type_cfg = _get_page_type_config(doc_type, page_type_name)
        fields_result = []
        if page_type_cfg.get("Fields"):
            textract_resp = _textract.analyze_document(
                Document={"Bytes": page_bytes},
                FeatureTypes=["FORMS", "TABLES"],
            )
            blocks = textract_resp.get("Blocks", [])
            extracted = extraction_logic.extract_fields_for_page(blocks, page_type_cfg)
            for f in page_type_cfg.get("Fields", []):
                name = f["FieldName"]
                match = extracted.get(name)
                fields_result.append({
                    "fieldName": name,
                    "value": match["value"] if match else None,
                    "confidence": match["confidence"] if match else None,
                    "source": match["source"] if match else None,
                    "matchedKeyword": match.get("matchedKeyword") if match else None,
                })

        page_results.append({
            "pageNumber": idx,
            "pageType": page_type_name,
            "fields": fields_result,
            "previewImageBase64": _make_preview_image(page_bytes),
        })

    return {
        "applicationId": application_id,
        "documentTypeId": document_type_id,
        "documentTypeVersion": doc_type.get("version"),
        "classificationMode": mode,
        "fileName": file_name,
        "pageCount": len(page_images),
        "pages": page_results,
    }
