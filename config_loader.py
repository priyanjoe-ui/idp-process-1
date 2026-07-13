"""
Config loader for the IDP pipeline.

CHANGED: previously read a static document-config.json from S3. That file
is a snapshot from before Studio existed -- Studio writes Applications /
Document Types / Dictionaries to the `idp-config` DynamoDB table, and
idp-api-proxy's own loadConfig()/assembleFullConfig() already reads that
table for the reviewer app and dashboard. Nothing wrote Studio's edits
back to the S3 file, so this Lambda layer was silently reading
increasingly stale config. This version reads the same table + GSI the
proxy does, so Studio edits reach the runtime pipeline the same way they
already reach the reviewer app.

Public surface (load_config, get_application, invalidate) is unchanged --
callers in idp-classification / idp-extraction-callback / idp-validation
don't need to change anything.

Row shapes (must match idp-api-proxy's Studio schema exactly):
  PK               SK              type          body
  "APP#<appId>"    "META"          APPLICATION   { ApplicationName, DisplayName?, ClassificationMode? }
  "APP#<appId>"    "DT#<dtId>"     DOC_TYPE       ConfigDocumentType (DocumentTypeName, PageTypes, ...)
  "DICT"           "<name>"        DICTIONARY     { options: FieldOption[] }

GSI (env: IDP_CONFIG_TYPE_INDEX, default "type-lastModifiedAt-index"):
  PK: type   SK: lastModifiedAt
  Same index idp-api-proxy's ddbConfigQueryByType() uses -- listing every
  row of a type is a GSI query, never a Scan.

Cached in module scope per Lambda cold start, same as the old S3 loader.
Call invalidate() to force a refresh without redeploying (there's no TTL
here, same as before -- recycle the Lambda, or wire invalidate() into a
handler if config needs to be picked up without a redeploy).
"""

import logging
import os
from typing import Any, Dict, List, Optional

import boto3
from boto3.dynamodb.conditions import Key

logger = logging.getLogger()

_ddb = boto3.resource("dynamodb")
_CACHE: Optional[Dict[str, Any]] = None


def _table():
    return _ddb.Table(os.environ.get("IDP_CONFIG_TABLE", "idp-config"))


def _type_index() -> str:
    return os.environ.get("IDP_CONFIG_TYPE_INDEX", "type-lastModifiedAt-index")


def _query_by_type(type_name: str) -> List[Dict[str, Any]]:
    """Every row of a given `type`, via the GSI. Mirrors idp-api-proxy's
    ddbConfigQueryByType() field-for-field (same index, same pagination
    loop) so both sides of the system see an identical row set."""
    items: List[Dict[str, Any]] = []
    kwargs: Dict[str, Any] = {
        "IndexName": _type_index(),
        "KeyConditionExpression": Key("type").eq(type_name),
    }
    while True:
        resp = _table().query(**kwargs)
        items.extend(resp.get("Items", []))
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break
        kwargs["ExclusiveStartKey"] = last_key
    return items


def _assemble_full_config() -> Dict[str, Any]:
    """Mirrors idp-api-proxy's assembleFullConfig() field-for-field: fetch
    APPLICATION / DOC_TYPE / DICTIONARY rows in parallel-equivalent
    fashion, group doc types under their parent app, and return the same
    {Applications, Dictionaries} shape the old S3 file had -- so
    get_application() below needs no changes."""
    app_rows = _query_by_type("APPLICATION")
    dt_rows = _query_by_type("DOC_TYPE")
    dict_rows = _query_by_type("DICTIONARY")

    doc_types_by_app: Dict[str, List[Dict[str, Any]]] = {}
    for dt in dt_rows:
        app_id = str(dt["PK"]).replace("APP#", "", 1)
        doc_types_by_app.setdefault(app_id, []).append(dt.get("body") or {})

    applications: List[Dict[str, Any]] = []
    for app in app_rows:
        app_id = str(app["PK"]).replace("APP#", "", 1)
        meta = app.get("body") or {}
        applications.append({
            "ApplicationName": meta.get("ApplicationName", app_id),
            "DisplayName": meta.get("DisplayName"),
            "ClassificationMode": meta.get("ClassificationMode", "Sequential"),
            "DocumentTypes": doc_types_by_app.get(app_id, []),
        })

    dictionaries: Dict[str, Any] = {}
    for d in dict_rows:
        name = str(d["SK"])
        dictionaries[name] = (d.get("body") or {}).get("options", [])

    return {"Applications": applications, "Dictionaries": dictionaries}


def load_config(force_refresh: bool = False) -> Dict[str, Any]:
    """Return the parsed config dict, using cold-start cache."""
    global _CACHE
    if _CACHE is not None and not force_refresh:
        return _CACHE

    logger.info(
        "Loading config from DynamoDB table=%s index=%s",
        os.environ.get("IDP_CONFIG_TABLE", "idp-config"),
        _type_index(),
    )
    _CACHE = _assemble_full_config()
    logger.info(
        "Config loaded: %d application(s), %d dictionary(ies)",
        len(_CACHE["Applications"]),
        len(_CACHE["Dictionaries"]),
    )
    return _CACHE


def get_application(app_name: str) -> Dict[str, Any]:
    """
    Return the application block for `app_name`.

    Unchanged from the old S3-backed loader: still supports the
    {"Applications": [...]} shape (the normal case now) and the legacy
    single-app shape, in case anything ever hands this a hand-built dict
    in that older shape (e.g. a local test fixture).
    """
    cfg = load_config()
    if "Applications" in cfg:
        for app in cfg["Applications"]:
            if app.get("ApplicationName") == app_name:
                return app
        raise KeyError(f"Application '{app_name}' not in config")
    if cfg.get("ApplicationName") == app_name:
        return cfg
    raise KeyError(f"Application '{app_name}' not in config (single-app shape)")


def invalidate() -> None:
    """Drop the cache. Next call to load_config() will re-fetch."""
    global _CACHE
    _CACHE = None
