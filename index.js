/**
* IDP - Local API Proxy
*
* Single Node.js process that bridges the Angular UI to AWS (DynamoDB, S3,
* Step Functions). The UI talks HTTP to this proxy at http://localhost:3001;
* this proxy uses the AWS SDK to query/update DDB and convert TIFFs to PNG.
*
* Run locally with:
*   node local-api-proxy.js
*
* AWS credentials are taken from the standard credential chain (AWS CLI
* `aws configure`, env vars, or instance role). No keys hardcoded.
*
* Endpoints
* ---------
*   GET  /api/applications
*   GET  /api/applications/:app/jobs
*   GET  /api/batches                              ?application=&status=&search=&page=&pageSize=&sortBy=&sortOrder=
*   GET  /api/batches/:queueId
*   GET  /api/batches/:queueId/pages/:pageId/image
*   POST /api/batches/:queueId/submit-review       body: {reviewer, corrections, lineItems}
*   POST /api/batches/:queueId/save-draft          body: {reviewer, corrections, lineItems}
*   POST /api/batches/:queueId/hold                body: {operator, reason}
*   POST /api/batches/:queueId/release             body: {operator}
*   GET  /api/auth/me                              header: X-Operator
*   POST /api/auth/login                           body: {username}
*   GET  /api/health                               (proxy heartbeat)
*/
 
const express = require('express');
const cors = require('cors');
const {
  DynamoDBClient,
} = require('@aws-sdk/client-dynamodb');
const {
  DynamoDBDocumentClient,
  QueryCommand,
  GetCommand,
  UpdateCommand,
  PutCommand,
  DeleteCommand,
  ScanCommand,
} = require('@aws-sdk/lib-dynamodb');
const {
  S3Client,
  GetObjectCommand,
  ListBucketsCommand,
  ListObjectsV2Command,
  PutObjectCommand,
} = require('@aws-sdk/client-s3');
const {
  SFNClient,
  SendTaskSuccessCommand,
  CreateStateMachineCommand,
  UpdateStateMachineCommand,
  DescribeStateMachineCommand,
} = require('@aws-sdk/client-sfn');
const {
  DescribeTableCommand,
} = require('@aws-sdk/client-dynamodb');
const {
  CloudWatchClient,
  GetMetricStatisticsCommand,
} = require('@aws-sdk/client-cloudwatch');
const {
  SQSClient,
  GetQueueAttributesCommand,
} = require('@aws-sdk/client-sqs');
const {
  LambdaClient,
  GetFunctionConfigurationCommand,
} = require('@aws-sdk/client-lambda');
const sharp = require('sharp');
const UTIF = require('utif');
const { PNG } = require('pngjs');
 
// -----------------------------------------------------------------------
// Configuration
// -----------------------------------------------------------------------
const REGION       = process.env.AWS_REGION         || 'us-east-1';
const TABLE        = process.env.IDP_TABLE    || 'idp-batch-tracking';
const CONFIG_TABLE = process.env.IDP_CONFIG_TABLE   || 'idp-config';
const CONFIG_TYPE_INDEX = process.env.IDP_CONFIG_TYPE_INDEX || 'type-lastModifiedAt-index';
const DOCS_BUCKET  = process.env.DOCS_BUCKET        || 'idp-ecm-datacap-dev';
const CONFIG_KEY   = process.env.CONFIG_KEY         || 'idp/config/document-config.json';
const STUDIO_USER  = process.env.STUDIO_USER        || 'studio-admin';
/**
 * Reused for CreateStateMachine's roleArn. The account's IAM boundary
 * policy denies iam:CreateRole — this MUST be an existing role Step
 * Functions can already assume (trust policy allows states.amazonaws.com),
 * never a role created for this purpose.
 */
const WORKFLOW_ROLE_ARN = process.env.WORKFLOW_ROLE_ARN
  || 'arn:aws:iam::651720177345:role/LambdaExecutionRole';
const PORT         = parseInt(process.env.PROXY_PORT || '3001', 10);

// Service-health check targets — these need real values set per environment;
// the fallbacks below just match this project's established naming
// convention, not necessarily what's actually deployed.
const INTAKE_QUEUE_URL = process.env.INTAKE_QUEUE_URL || '';
const HEALTH_LAMBDA_FUNCTION_NAMES = (process.env.HEALTH_LAMBDA_FUNCTION_NAMES || '')
  .split(',').map(s => s.trim()).filter(Boolean);

const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({ region: REGION }), {
  marshallOptions: { removeUndefinedValues: true },
});
const s3  = new S3Client({ region: REGION });
const sfn = new SFNClient({ region: REGION });
const cloudwatch = new CloudWatchClient({ region: REGION });
const sqsClient = new SQSClient({ region: REGION });
const lambdaClient = new LambdaClient({ region: REGION });
const dynamodbClient = new DynamoDBClient({ region: REGION });
 
// In-memory caches
const CONFIG_TTL_MS = 5 * 60 * 1000;
let _configCache = null;
 
const IMAGE_CACHE_TTL_MS = 10 * 60 * 1000;
const _imageCache = new Map();   // s3Key -> { buffer, expiresAt }
 
// -----------------------------------------------------------------------
// Express setup
// -----------------------------------------------------------------------
const app = express();
 
app.use(express.json({ limit: '2mb' }));
 
app.use((req, _res, next) => {
  console.log(`${new Date().toISOString()} ${req.method} ${req.originalUrl}`);
  next();
});
 
// -----------------------------------------------------------------------
// Utilities
// -----------------------------------------------------------------------
function jsonError(res, status, code, message) {
  return res.status(status).json({ error: code, message });
}
 
function safeNumber(v, dflt) {
  const n = parseInt(v, 10);
  return Number.isFinite(n) ? n : dflt;
}
 
function getOperator(req) {
  const h = req.header('X-Operator');
  return h || 'anonymous';
}
 
function nowIso() {
  return new Date().toISOString();
}
 
function secondsSince(iso) {
  if (!iso) return null;
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return null;
  return Math.max(0, Math.floor((Date.now() - t) / 1000));
}
 
function durationMs(startIso, endIso) {
  if (!startIso || !endIso) return null;
  const a = Date.parse(startIso);
  const b = Date.parse(endIso);
  if (Number.isNaN(a) || Number.isNaN(b)) return null;
  return Math.max(0, b - a);
}
 
/** Convert wildcard pattern (COR*) to a case-insensitive RegExp. */
function wildcardToRegex(pattern) {
  if (!pattern) return null;
  const escaped = pattern.replace(/[.+^${}()|[\]\\]/g, '\\$&');
  const withWildcard = escaped.replace(/\*/g, '.*').replace(/\?/g, '.');
  return new RegExp('^' + withWildcard + '$', 'i');
}
 
// -----------------------------------------------------------------------
// Config loader (document-config.json from S3)
// -----------------------------------------------------------------------
// -----------------------------------------------------------------------
// Config load — assembles the nested Applications/DocumentTypes/PageTypes/
// Fields tree by querying the idp-config DDB table. Same shape as the
// legacy S3-backed loader so every caller (reviewer app, dashboard, submit
// flow) keeps working without changes.
//
// Uses the GSI to fetch APPLICATION, DOC_TYPE, and DICTIONARY rows in
// parallel, then groups doc types under their parent app.
// -----------------------------------------------------------------------
async function loadConfig() {
  if (_configCache && _configCache.expiresAt > Date.now()) {
    return _configCache.cfg;
  }
  const cfg = await assembleFullConfig();
  _configCache = { cfg, expiresAt: Date.now() + CONFIG_TTL_MS };
  return cfg;
}

async function assembleFullConfig() {
  const [appRows, dtRows, dictRows] = await Promise.all([
    ddbConfigQueryByType('APPLICATION'),
    ddbConfigQueryByType('DOC_TYPE'),
    ddbConfigQueryByType('DICTIONARY'),
  ]);

  const docTypesByApp = {};
  for (const dt of dtRows) {
    const appId = String(dt.PK).replace(/^APP#/, '');
    (docTypesByApp[appId] = docTypesByApp[appId] || []).push(dt.body || {});
  }

  const Applications = appRows.map(app => {
    const appId = String(app.PK).replace(/^APP#/, '');
    const meta = app.body || {};
    return {
      ApplicationName:    meta.ApplicationName || appId,
      DisplayName:        meta.DisplayName,
      ClassificationMode: meta.ClassificationMode || 'Sequential',
      DocumentTypes:      docTypesByApp[appId] || [],
    };
  });

  const Dictionaries = {};
  for (const d of dictRows) {
    const name = String(d.SK);
    Dictionaries[name] = (d.body && d.body.options) || [];
  }

  return { Applications, Dictionaries };
}

function invalidateConfigCache() {
  _configCache = null;
}
 
function applicationByName(cfg, name) {
  const apps = cfg.Applications || cfg.applications || [];
  return apps.find(a => a.ApplicationName === name || a.applicationName === name);
}
 
// -----------------------------------------------------------------------
// DDB helpers
// -----------------------------------------------------------------------
async function getBatchMeta(queueId) {
  const resp = await ddb.send(new GetCommand({
    TableName: TABLE,
    Key: { PK: `BATCH#${queueId}`, SK: 'META' },
  }));
  return resp.Item || null;
}
 
async function queryBatchAll(queueId) {
  const resp = await ddb.send(new QueryCommand({
    TableName: TABLE,
    KeyConditionExpression: 'PK = :pk',
    ExpressionAttributeValues: { ':pk': `BATCH#${queueId}` },
  }));
  return resp.Items || [];
}
 
/**
 * Stage 8: persist tree-state changes to DDB.
 *
 * The UI sends the FINAL desired state (full pages + documents arrays).
 * This function reads the current DDB state, diffs against what was
 * sent, and applies the differences:
 *
 *   - Documents present in desired but not in DDB     -> create (PutCommand)
 *   - Documents in DDB but not in desired             -> delete (DeleteCommand)
 *   - Documents in both with different pageIds        -> update pageIds + pageType ordering
 *   - Pages in DDB but not in desired                 -> delete (DeleteCommand)
 *   - Pages in both with different pageNumber/pageType -> update those attrs
 *
 * Also recomputes batch.documentCount + pageCount and writes them to META.
 *
 * Sequential writes (no transaction) per user direction. Failures are
 * logged and re-thrown so the caller can surface 500 to the UI. Partial
 * commits are possible if a later write fails after an earlier one
 * succeeded — for POC scale this is acceptable; production would want
 * TransactWriteItems.
 *
 * Returns a summary of what changed for audit logging.
 */
async function persistTreeState(queueId, desiredDocs, desiredPages) {
  if (!Array.isArray(desiredDocs) || !Array.isArray(desiredPages)) {
    throw new Error('persistTreeState: desiredDocs and desiredPages must be arrays');
  }
 
  // Load current state.
  const rawItems = await queryBatchAll(queueId);
  const currentDocs  = rawItems.filter(i => i.SK && i.SK.startsWith('DOC#'));
  const currentPages = rawItems.filter(i => i.SK && i.SK.startsWith('PAGE#'));
 
  // Build lookup maps keyed by id.
  const currentDocById  = new Map(currentDocs.map(d  => [d.documentId, d]));
  const currentPageById = new Map(currentPages.map(p => [p.pageId, p]));
  const desiredDocIds   = new Set(desiredDocs.map(d => d.documentId));
  const desiredPageIds  = new Set(desiredPages.map(p => p.pageId));
 
  const summary = {
    docsCreated:  0,
    docsUpdated:  0,
    docsDeleted:  0,
    pagesUpdated: 0,
    pagesDeleted: 0,
  };
 
  // ----- Pages: deletions and attribute updates -----
  // Walk DDB pages: any not in desired -> delete.
  for (const pageItem of currentPages) {
    if (!desiredPageIds.has(pageItem.pageId)) {
      await ddb.send(new DeleteCommand({
        TableName: TABLE,
        Key: { PK: pageItem.PK, SK: pageItem.SK },
      }));
      summary.pagesDeleted++;
    }
  }
 
  // Walk desired pages: any whose pageNumber/pageType differ from DDB -> update.
  // We only touch the attributes the UI tree-ops can change. Other
  // attributes (s3Key, ocrTextS3Key, imageUrl, etc.) are untouched.
  for (const desiredPage of desiredPages) {
    const cur = currentPageById.get(desiredPage.pageId);
    if (!cur) {
      // Page in UI but not in DDB. The UI never creates new pages
      // (only splits docs / moves existing pages), so this should be
      // impossible. Log and skip rather than fabricate a page record.
      console.warn(`[STAGE8] persistTreeState: desired pageId=${desiredPage.pageId} has no DDB row, skipping`);
      continue;
    }
    const newPageNum  = Number(desiredPage.pageNumber);
    const newPageType = desiredPage.pageType;
    if (Number(cur.pageNumber) === newPageNum && cur.pageType === newPageType) {
      continue;  // no change
    }
    await ddb.send(new UpdateCommand({
      TableName: TABLE,
      Key: { PK: cur.PK, SK: cur.SK },
      UpdateExpression: 'SET pageNumber = :pn, pageType = :pt, updatedAt = :now',
      ExpressionAttributeValues: {
        ':pn':  newPageNum,
        ':pt':  newPageType,
        ':now': nowIso(),
      },
    }));
    summary.pagesUpdated++;
  }
 
  // ----- Documents: deletions, creations, and pageIds updates -----
  // Delete docs that are gone from desired.
  for (const docItem of currentDocs) {
    if (!desiredDocIds.has(docItem.documentId)) {
      await ddb.send(new DeleteCommand({
        TableName: TABLE,
        Key: { PK: docItem.PK, SK: docItem.SK },
      }));
      summary.docsDeleted++;
    }
  }
 
  // Create or update docs that are in desired.
  for (const desiredDoc of desiredDocs) {
    const cur = currentDocById.get(desiredDoc.documentId);
    if (!cur) {
      // New document (created by a split). Build a minimal DOC# record.
      // Field/lineItem content is populated by the existing submit-review
      // / save-draft flow which runs AFTER persistTreeState, so we start
      // these out empty here.
      const ts = nowIso();
      await ddb.send(new PutCommand({
        TableName: TABLE,
        Item: {
          PK:                `BATCH#${queueId}`,
          SK:                `DOC#${desiredDoc.documentId}`,
          Type:              'DOC',
          documentId:        desiredDoc.documentId,
          documentType:      desiredDoc.documentType || 'Unclassified',
          validationStatus:  desiredDoc.validationStatus || 'NeedsReview',
          pageIds:           desiredDoc.pageIds || [],
          extractedFields:   [],
          lineItems:         [],
          createdAt:         ts,
          updatedAt:         ts,
        },
      }));
      summary.docsCreated++;
      continue;
    }
    // Existing doc - only touch pageIds (the only attribute tree ops change).
    const curPageIds     = JSON.stringify(cur.pageIds || []);
    const desiredPageIds = JSON.stringify(desiredDoc.pageIds || []);
    if (curPageIds === desiredPageIds) {
      continue;  // pageIds unchanged
    }
    await ddb.send(new UpdateCommand({
      TableName: TABLE,
      Key: { PK: cur.PK, SK: cur.SK },
      UpdateExpression: 'SET pageIds = :pi, updatedAt = :now',
      ExpressionAttributeValues: {
        ':pi':  desiredDoc.pageIds || [],
        ':now': nowIso(),
      },
    }));
    summary.docsUpdated++;
  }
 
  // ----- Batch META: update documentCount + pageCount if changed -----
  const meta = rawItems.find(i => i.SK === 'META');
  if (meta) {
    const newDocCount  = desiredDocs.length;
    const newPageCount = desiredPages.length;
    if (Number(meta.documentCount) !== newDocCount || Number(meta.pageCount) !== newPageCount) {
      await ddb.send(new UpdateCommand({
        TableName: TABLE,
        Key: { PK: meta.PK, SK: meta.SK },
        UpdateExpression: 'SET documentCount = :dc, pageCount = :pc, updatedAt = :now',
        ExpressionAttributeValues: {
          ':dc':  newDocCount,
          ':pc':  newPageCount,
          ':now': nowIso(),
        },
      }));
    }
  }
 
  return summary;
}
 
/** Query the BatchByStatus GSI for batches with a given status. */
async function queryByStatus(status, limit = 500) {
  const resp = await ddb.send(new QueryCommand({
    TableName: TABLE,
    IndexName: 'GSI1-BatchByStatus',
    KeyConditionExpression: 'GSI1PK = :pk',
    ExpressionAttributeValues: { ':pk': `STATUS#${status}` },
    ScanIndexForward: false,  // newest first
    Limit: limit,
  }));
  return resp.Items || [];
}
 
/** Query the BatchByApp GSI for batches in a given application. */
async function queryByApp(application, limit = 1000) {
  const resp = await ddb.send(new QueryCommand({
    TableName: TABLE,
    IndexName: 'GSI2-BatchByApp',
    KeyConditionExpression: 'GSI2PK = :pk',
    ExpressionAttributeValues: { ':pk': `APP#${application}` },
    ScanIndexForward: false,  // newest first
    Limit: limit,
  }));
  return resp.Items || [];
}
 
// -----------------------------------------------------------------------
// Batch shaping
// -----------------------------------------------------------------------
/**
* Compute Job Time (overall processing duration in seconds).
*
* Rule:
*  - status Running or Pending → live wall clock since jobStartTime
*  - any other state (OnHold, JobDone, Failed, Aborted, Finished) → freeze at
*    the last task transition. Uses `lastTaskUpdatedAt` if present; otherwise
*    falls back to `meta.updatedAt` (a reasonable approximation for old batches
*    written before this field was maintained).
*/
function computeJobTime(meta) {
  const start = meta.jobStartTime;
  if (!start) return null;
  const startMs = Date.parse(start);
  if (Number.isNaN(startMs)) return null;
 
  const active = meta.status === 'Running' || meta.status === 'Pending';
  if (active) {
    return Math.max(0, Math.floor((Date.now() - startMs) / 1000));
  }
 
  // Frozen: use lastTaskUpdatedAt if available, else fall back to updatedAt
  const freezeIso = meta.lastTaskUpdatedAt || meta.updatedAt || start;
  const freezeMs = Date.parse(freezeIso);
  if (Number.isNaN(freezeMs)) return null;
  return Math.max(0, Math.floor((freezeMs - startMs) / 1000));
}
 
/**
 * Stage 9: normalize legacy status values on read with semantic distinction.
 *
 * The new taxonomy:
 *   - "pending" = system routed this batch to manual review (validation
 *                 failure / low confidence). Set by idp-manual-review.
 *   - "hold"    = user-initiated Hold from the Verify screen. Set by
 *                 POST /api/batches/:queueId/hold (this file).
 *
 * Legacy DDB rows have a single "OnHold" value, with holdSource telling us
 * which path they came from:
 *   - holdSource === 'manualReview' (or missing on old rows from before that
 *     attribute existed) -> rewrite OnHold -> pending
 *   - holdSource === 'manual'                                    -> rewrite OnHold -> hold
 *
 * Background: pre-stage-7 manual-review code set holdSource='manualReview'
 * but the field was added relatively late; truly old rows have no
 * holdSource at all. Those were all system-routed (the manual-hold endpoint
 * is recent), so treating missing-holdSource as 'manualReview' is correct.
 *
 * This read-side helper keeps the UI consistent during/after the
 * `scripts/migrate-onhold-by-source.py` rollout.
 */
function normalizeStatus(meta) {
  const s = meta.status;
  if (s === 'OnHold') {
    return meta.holdSource === 'manual' ? 'hold' : 'pending';
  }
  return s;
}
 
function shapeBatchRow(meta) {
  const start = meta.jobStartTime;
  const status = normalizeStatus(meta);
  const isTerminal = status === 'JobDone' || status === 'Failed';
  const endRef = isTerminal ? (meta.updatedAt || start) : nowIso();
  return {
    queueId: Number(meta.queueId),
    batchId: meta.batchId,
    batchNumber: meta.batchNumber,
    application: meta.application,
    source: meta.source,
    jobName: meta.jobName,
    currentTask: meta.currentTask,
    status: status,
    jobStartTime: start,
    jobTime: computeJobTime(meta),
    jobDuration: durationMs(start, endRef),
    operator: meta.operator,
    documentCount: Number(meta.documentCount || 0),
    pageCount: Number(meta.pageCount || 0),
    updatedAt: meta.updatedAt,
  };
}
 
function shapeDocument(doc) {
  // Stage 5: line items are stored as a list of column-name -> value maps
  // on the DOC# record. Old batches that pre-date Stage 5 won't have the
  // attribute - we coerce missing/non-array values to [] so the UI always
  // sees a consistent shape.
  const rawLineItems = Array.isArray(doc.lineItems) ? doc.lineItems : [];
  const lineItems = rawLineItems.map(row => {
    if (!row || typeof row !== 'object') return {};
    // Stringify every cell value defensively - DDB returns Numbers and
    // Decimals for some cells (e.g. policy numbers stored numerically),
    // and the UI grid is string-typed.
    const out = {};
    for (const k of Object.keys(row)) {
      const v = row[k];
      out[k] = v == null ? '' : String(v);
    }
    return out;
  });
 
  return {
    documentId: doc.documentId,
    documentType: doc.documentType,
    validationStatus: doc.validationStatus,
    pageIds: doc.pageIds || [],
    extractedFields: (doc.extractedFields || []).map(f => ({
      fieldName: f.fieldName,
      value: f.value == null ? null : String(f.value),
      confidence: f.confidence == null ? null : Number(f.confidence),
      validationPassed: f.validationPassed === true,
      sourcePageId: f.sourcePageId || null,
      required: f.required === true,
      dataType: f.dataType || 'String',
      validationError: f.validationError || null,
      correctedBy: f.correctedBy || null,
      correctedAt: f.correctedAt || null,
    })),
    lineItems,
    createdAt: doc.createdAt,
    updatedAt: doc.updatedAt,
  };
}
 
function shapePage(page, queueId) {
  return {
    pageId: page.pageId,
    displayId: page.displayId || null,
    pageNumber: Number(page.pageNumber || 0),
    pageType: page.pageType,
    pageStatus: page.pageStatus,
    wordCount: page.wordCount == null ? null : Number(page.wordCount),
    s3Key: page.s3Key,
    ocrTextS3Key: page.ocrTextS3Key || null,
    imageUrl: `${process.env.API_URL || ''}/batches/${queueId}/pages/${encodeURIComponent(page.pageId)}/image`,
    createdAt: page.createdAt,
    updatedAt: page.updatedAt,
  };
}
 
 
function shapeTaskHistory(t) {
  return {
    taskName: t.taskName,
    status: t.status,
    startTime: t.startTime,
    endTime: t.endTime,
    operator: t.operator,
    durationMs: durationMs(t.startTime, t.endTime),
    details: t.details || null,
  };
}
 
function shapeTextractJob(j) {
  return {
    jobId: j.jobId,
    pageId: j.pageId,
    status: j.status,
    createdAt: j.createdAt,
    completedAt: j.completedAt || null,
    durationMs: durationMs(j.createdAt, j.completedAt),
    resultS3Key: j.resultS3Key || null,
    errorMessage: j.errorMessage || null,
  };
}
 
// -----------------------------------------------------------------------
// TIFF -> PNG conversion (preserved from previous proxy)
// -----------------------------------------------------------------------
async function fetchS3ObjectBytes(bucket, key) {
  const cached = _imageCache.get(key);
  if (cached && cached.expiresAt > Date.now()) return cached.buffer;
 
  const resp = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
  const chunks = [];
  for await (const chunk of resp.Body) chunks.push(chunk);
  const buf = Buffer.concat(chunks);
  return buf;
}
 
/**
* Convert a TIFF buffer to a PNG buffer. Tries `sharp` first (fast, handles
* common compressions), falls back to UTIF+pngjs for fringe cases.
*/
async function tiffToPng(tiffBuffer) {
  // Try sharp first
  try {
    const png = await sharp(tiffBuffer, { failOn: 'none' })
      .png({ compressionLevel: 6 })
      .toBuffer();
    return png;
  } catch (sharpErr) {
    console.warn('sharp failed, falling back to UTIF:', sharpErr.message);
  }
 
  // UTIF fallback
  const ifds = UTIF.decode(tiffBuffer);
  if (!ifds || ifds.length === 0) {
    throw new Error('UTIF could not decode TIFF');
  }
  const ifd = ifds[0];
  UTIF.decodeImage(tiffBuffer, ifd);
  const rgba = UTIF.toRGBA8(ifd);
  const png = new PNG({ width: ifd.width, height: ifd.height });
  for (let i = 0; i < rgba.length; i++) png.data[i] = rgba[i];
  return PNG.sync.write(png);
}
 
async function getPageImagePng(s3Key) {
  const cached = _imageCache.get(s3Key);
  if (cached && cached.expiresAt > Date.now()) return cached.buffer;
  const tiffBuf = await fetchS3ObjectBytes(DOCS_BUCKET, s3Key);
  const pngBuf = await tiffToPng(tiffBuf);
  _imageCache.set(s3Key, { buffer: pngBuf, expiresAt: Date.now() + IMAGE_CACHE_TTL_MS });
  return pngBuf;
}
 
setInterval(() => {
  const now = Date.now();
  for (const [k, v] of _imageCache.entries()) {
    if (v.expiresAt <= now) _imageCache.delete(k);
  }
}, 60_000).unref?.();
 
// -----------------------------------------------------------------------
// OCR word loading (for Click N + Search features)
// -----------------------------------------------------------------------
// Per-page OCR JSON cache. OCR files don't change after extraction, so a
// long TTL is fine. Keyed by S3 key.
const OCR_CACHE_TTL_MS = 30 * 60 * 1000;
const _ocrCache = new Map();   // s3Key -> { words, expiresAt }
 
setInterval(() => {
  const now = Date.now();
  for (const [k, v] of _ocrCache.entries()) {
    if (v.expiresAt <= now) _ocrCache.delete(k);
  }
}, 5 * 60_000).unref?.();
 
/**
* Read one OCR JSON file from S3 and return its WORD blocks slimmed to
* { text, bbox: {l, t, w, h} }. Returns [] for any error (missing key,
* stripped-geometry old file, parse failure) so a single bad page doesn't
* fail the whole batch detail call.
*/
async function loadOcrWords(s3Key) {
  if (!s3Key) return [];
  const cached = _ocrCache.get(s3Key);
  if (cached && cached.expiresAt > Date.now()) return cached.words;
 
  try {
    const resp = await s3.send(new GetObjectCommand({
      Bucket: DOCS_BUCKET,
      Key: s3Key,
    }));
    const chunks = [];
    for await (const chunk of resp.Body) chunks.push(chunk);
    const text = Buffer.concat(chunks).toString('utf-8');
    const ocr = JSON.parse(text);
    const blocks = ocr.Blocks || [];
 
    const words = [];
    for (const b of blocks) {
      if (b.BlockType !== 'WORD') continue;
      const bbox = b.Geometry && b.Geometry.BoundingBox;
      if (!bbox) continue;              // Pre-fix OCR files have no geometry
      const wordText = b.Text;
      if (!wordText) continue;
      words.push({
        text: wordText,
        bbox: {
          l: bbox.Left,
          t: bbox.Top,
          w: bbox.Width,
          h: bbox.Height,
        },
      });
    }
 
    _ocrCache.set(s3Key, { words, expiresAt: Date.now() + OCR_CACHE_TTL_MS });
    return words;
  } catch (e) {
    console.warn(`loadOcrWords failed for ${s3Key}:`, e.message);
    return [];
  }
}
 
// =======================================================================
// ENDPOINTS
// =======================================================================
 
// -----------------------------------------------------------------------
// Health
// -----------------------------------------------------------------------
app.get('/api/health', (_req, res) => {
  res.json({ ok: true, ts: nowIso(), region: REGION, table: TABLE });
});
 
// -----------------------------------------------------------------------
// Auth (mock)
// -----------------------------------------------------------------------
app.post('/api/auth/login', (req, res) => {
  const username = (req.body && req.body.username) || '';
  if (!username || username.length < 1) {
    return jsonError(res, 400, 'BadRequest', 'username is required');
  }
  res.json({ operator: username, authenticated: true });
});
 
app.get('/api/auth/me', (req, res) => {
  const op = req.header('X-Operator');
  res.json({ operator: op || null, authenticated: !!op });
});
 
// -----------------------------------------------------------------------
// Applications
// -----------------------------------------------------------------------
// Returns the full application configuration including field definitions
// and shared dictionaries so the UI can render dropdowns, line items, and
// auto-populated fields directly from config (no hardcoded UI logic).
app.get('/api/applications', async (_req, res) => {
  try {
    const cfg = await loadConfig();
    const apps = cfg.Applications || cfg.applications || [];
    res.json({
      applications: apps.map(a => ({
        name: a.ApplicationName || a.applicationName,
        displayName: a.DisplayName || a.displayName || a.ApplicationName || a.applicationName,
        classificationMode: a.ClassificationMode || 'Sequential',
        documentTypes: (a.DocumentTypes || a.documentTypes || []).map(dt => ({
          name: dt.DocumentTypeName || dt.documentTypeName,
          pageTypes: (dt.PageTypes || []).map(pt => ({
            pageType: pt.PageType,
            fields: pt.Fields || [],
            // Array — a page can have any number of Line Items tables.
            // NOTE: this is a wire-format change. The old shape here was a
            // single object (or null); the reviewer app's rendering code
            // will need to iterate an array now instead of using one object
            // directly, if/when it's updated to consume this.
            lineItems: pt.LineItems || [],
            keywords: pt.Keywords || [],
          })),
        })),
      })),
      dictionaries: cfg.Dictionaries || {},
    });
  } catch (e) {
    console.error('GET /api/applications error:', e);
    jsonError(res, 500, 'InternalError', e.message);
  }
});
 
app.get('/api/applications/:app/jobs', async (req, res) => {
  try {
    const application = req.params.app;
    const items = await queryByApp(application, 1000);
 
    const jobs = {};
    for (const m of items) {
      const jn = m.jobName || `${application}-ImportJob`;
      if (!jobs[jn]) {
        jobs[jn] = { jobName: jn, counts: { total: 0 } };
      }
      jobs[jn].counts.total += 1;
      // Issue 1: normalize before counting so legacy OnHold rows get
      // bucketed into "hold" or "pending" by holdSource, matching what
      // the UI filter chips expect.
      const s = normalizeStatus(m) || 'Unknown';
      jobs[jn].counts[s] = (jobs[jn].counts[s] || 0) + 1;
    }
 
    res.json({
      application,
      jobs: Object.values(jobs),
    });
  } catch (e) {
    console.error('GET /api/applications/:app/jobs error:', e);
    jsonError(res, 500, 'InternalError', e.message);
  }
});
 
// -----------------------------------------------------------------------
// Dashboard
// -----------------------------------------------------------------------
// Server-side computation of all dashboard metrics, filtered by application
// and date range. Filter is applied to `jobStartTime`.
//
// Query params:
//   application  - 'AFLCOR' or omit / 'all' to include every application
//   from         - ISO date or yyyymmdd (start of range, inclusive)
//   to           - ISO date or yyyymmdd (end of range, inclusive)
//
// Returns:
//   {
//     totalIngested,         // count of batches whose jobStartTime falls in range
//     pendingReview,         // count currently in OnHold / ManualReview state
//     stpBatches,            // status=JobDone AND no ManualReview in history
//     abortedBatches,        // status in (Failed, Aborted)
//     stpPercentage,         // stpBatches / totalIngested * 100
//     byDocumentType: [{ type, count }],
//     byStatus: [{ status, count }],
//     recentTimeline: [{ bucket, processed, failed }]
//   }
function _parseRangeDate(v, endOfDay) {
  if (!v) return null;
  // Accept yyyymmdd (e.g. 20260518) or ISO date / datetime
  let s = String(v);
  if (/^\d{8}$/.test(s)) {
    s = `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}`;
  }
  const d = new Date(s);
  if (Number.isNaN(d.getTime())) return null;
  if (endOfDay && /^\d{4}-\d{2}-\d{2}$/.test(s)) {
    // Only YYYY-MM-DD was provided → push to end of day
    d.setUTCHours(23, 59, 59, 999);
  }
  return d.toISOString();
}
 
function _inRange(iso, fromIso, toIso) {
  if (!iso) return false;
  if (fromIso && iso < fromIso) return false;
  if (toIso   && iso > toIso)   return false;
  return true;
}
 
app.get('/api/dashboard', async (req, res) => {
  try {
    const application = (req.query.application || '').trim();
    const fromIso = _parseRangeDate(req.query.from, false);
    const toIso   = _parseRangeDate(req.query.to,   true);
 
    // 1. Fetch batches scoped by application
    let items;
    if (application && application.toLowerCase() !== 'all') {
      items = await queryByApp(application, 5000);
    } else {
      // No app filter — collect from all configured apps (config is cached).
      const cfg = await loadConfig();
      const apps = cfg.Applications || cfg.applications || [];
      const buckets = await Promise.all(apps.map(a =>
        queryByApp(a.ApplicationName || a.applicationName, 5000)
      ));
      items = buckets.flat();
    }
 
    // 2. Filter by jobStartTime range
    if (fromIso || toIso) {
      items = items.filter(b => _inRange(b.jobStartTime, fromIso, toIso));
    }
 
    const totalIngested = items.length;
 
    // 3. Pending review = anything currently waiting on human attention:
    //    - status "pending"  (system routed it to ManualReview)
    //    - status "hold"     (user clicked Hold on the Verify screen)
    //    - status "OnHold"   (legacy rows pre-migration, either of the above)
    //    - currentTask "ManualReview" as a backstop in case status is stale.
    const pendingReview = items.filter(b =>
      b.status === 'pending' ||
      b.status === 'hold' ||
      b.status === 'OnHold' ||
      b.currentTask === 'ManualReview'
    ).length;
 
    // 4. STP = status JobDone AND never went through ManualReview.
    //    We use the META row's `manualReviewStartedAt` field as the marker
    //    (set when ManualReview is entered, cleared field absent means STP).
    const stpBatches = items.filter(b =>
      b.status === 'JobDone' && !b.manualReviewStartedAt
    ).length;
 
    // 5. Aborted = Failed + Aborted statuses
    const abortedBatches = items.filter(b =>
      b.status === 'Failed' || b.status === 'Aborted'
    ).length;
 
    // 6. STP Percentage = STP / Total Ingested * 100
    const stpPercentage = totalIngested > 0
      ? Math.round((stpBatches / totalIngested) * 1000) / 10  // 1 decimal
      : 0;
 
    // 7. By Document Type — pull from DOC rows on each batch. We don't load
    //    them eagerly; instead use the application's configured document type
    //    name when batches don't carry a docType (Phase 1: 1 type per app).
    const docTypeCounts = {};
    for (const b of items) {
      const t = b.documentType || 'CORRDOC';
      docTypeCounts[t] = (docTypeCounts[t] || 0) + 1;
    }
    const byDocumentType = Object.entries(docTypeCounts)
      .map(([type, count]) => ({ type, count }))
      .sort((a, b) => b.count - a.count);
 
    // 8. By Status
    const statusCounts = {};
    for (const b of items) {
      const s = b.status || 'Unknown';
      statusCounts[s] = (statusCounts[s] || 0) + 1;
    }
    const byStatus = Object.entries(statusCounts)
      .map(([status, count]) => ({ status, count }))
      .sort((a, b) => b.count - a.count);
 
    // 9. Throughput timeline — bucket by hour over the filtered range.
    //    Use ISO 'YYYY-MM-DD HH' as the bucket key.
    const buckets = {};
    for (const b of items) {
      if (!b.jobStartTime) continue;
      const d = new Date(b.jobStartTime);
      if (Number.isNaN(d.getTime())) continue;
      const key = `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')} ${String(d.getUTCHours()).padStart(2, '0')}:00`;
      if (!buckets[key]) buckets[key] = { bucket: key, processed: 0, failed: 0 };
      if (b.status === 'Failed' || b.status === 'Aborted') {
        buckets[key].failed += 1;
      } else {
        buckets[key].processed += 1;
      }
    }
    const recentTimeline = Object.values(buckets)
      .sort((a, b) => a.bucket.localeCompare(b.bucket));
 
    res.json({
      filter: {
        application: application || 'all',
        from: fromIso,
        to: toIso,
      },
      totalIngested,
      pendingReview,
      stpBatches,
      abortedBatches,
      stpPercentage,
      byDocumentType,
      byStatus,
      recentTimeline,
    });
  } catch (e) {
    console.error('GET /api/dashboard error:', e);
    jsonError(res, 500, 'InternalError', e.message);
  }
});

// -----------------------------------------------------------------------
// Dashboard — Service Health
// -----------------------------------------------------------------------
//
// Five independent checks. Each one is wrapped in its own try/catch so a
// missing IAM permission or a misconfigured resource name for ONE service
// doesn't take down the other four — a real, partial result is always
// better than the whole endpoint failing because of one bad row.

const HEALTH_THRESHOLDS = {
  queueBackingUp: 100,       // messages visible
  queueStalled: 500,
  queueOldestBackingUpSec: 15 * 60,
  queueOldestStalledSec: 60 * 60,
  executionFailureDegradedPct: 10,
  lambdaErrorDegradedPct: 5,
};

async function checkConnectorsHealth() {
  const rows = await ddbConfigListAllConnectors();
  const enabled = rows.filter(r => r.body?.status === 'enabled').length;
  const disabled = rows.length - enabled;
  return {
    service: 'connectors',
    label: 'Connectors',
    status: 'info',
    detail: `${enabled} enabled · ${disabled} disabled`,
  };
}

async function checkQueueHealth() {
  if (!INTAKE_QUEUE_URL) {
    return { service: 'queue', label: 'Intake Queue', status: 'info', detail: 'Not configured (set INTAKE_QUEUE_URL).' };
  }
  const resp = await sqsClient.send(new GetQueueAttributesCommand({
    QueueUrl: INTAKE_QUEUE_URL,
    AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'],
  }));
  const visible = parseInt(resp.Attributes?.ApproximateNumberOfMessages || '0', 10);
  let status = 'healthy';
  if (visible >= HEALTH_THRESHOLDS.queueStalled) status = 'down';
  else if (visible >= HEALTH_THRESHOLDS.queueBackingUp) status = 'degraded';
  return {
    service: 'queue',
    label: 'Intake Queue',
    status,
    detail: `${visible} message${visible === 1 ? '' : 's'} waiting`,
  };
}

async function checkStepFunctionsHealth() {
  const wfResp = await ddb.send(new QueryCommand({
    TableName: CONFIG_TABLE,
    KeyConditionExpression: 'PK = :pk',
    ExpressionAttributeValues: { ':pk': 'WORKFLOW' },
  }));
  const deployed = (wfResp.Items || []).filter(w => w.body?.stateMachineArn);
  if (deployed.length === 0) {
    return { service: 'stepFunctions', label: 'Step Functions', status: 'info', detail: 'No deployed workflows yet.' };
  }

  let unreachable = 0;
  let degraded = 0;
  for (const wf of deployed) {
    const arn = wf.body.stateMachineArn;
    try {
      await sfn.send(new DescribeStateMachineCommand({ stateMachineArn: arn }));
    } catch {
      unreachable++;
      continue;
    }
    try {
      const [failed, succeeded] = await Promise.all([
        _sumMetric('AWS/States', 'ExecutionsFailed', [{ Name: 'StateMachineArn', Value: arn }]),
        _sumMetric('AWS/States', 'ExecutionsSucceeded', [{ Name: 'StateMachineArn', Value: arn }]),
      ]);
      const total = failed + succeeded;
      if (total > 0 && (failed / total) * 100 >= HEALTH_THRESHOLDS.executionFailureDegradedPct) degraded++;
    } catch {
      // Metric fetch failing doesn't mean the workflow is unhealthy — just
      // means we can't confirm either way, so it's not counted as degraded.
    }
  }

  let status = 'healthy';
  if (unreachable > 0) status = 'down';
  else if (degraded > 0) status = 'degraded';
  const detailParts = [];
  if (unreachable > 0) detailParts.push(`${unreachable} of ${deployed.length} unreachable`);
  if (degraded > 0) detailParts.push(`${degraded} of ${deployed.length} elevated failures`);
  return {
    service: 'stepFunctions',
    label: 'Step Functions',
    status,
    detail: detailParts.length ? detailParts.join(' · ') : `${deployed.length} workflow${deployed.length === 1 ? '' : 's'} healthy`,
  };
}

async function checkLambdaHealth() {
  if (HEALTH_LAMBDA_FUNCTION_NAMES.length === 0) {
    return { service: 'lambda', label: 'Lambda Functions', status: 'info', detail: 'Not configured (set HEALTH_LAMBDA_FUNCTION_NAMES).' };
  }
  let down = 0;
  let degraded = 0;
  for (const name of HEALTH_LAMBDA_FUNCTION_NAMES) {
    try {
      const cfg = await lambdaClient.send(new GetFunctionConfigurationCommand({ FunctionName: name }));
      if (cfg.State && cfg.State !== 'Active') { down++; continue; }
    } catch {
      down++;
      continue;
    }
    try {
      const [errors, invocations] = await Promise.all([
        _sumMetric('AWS/Lambda', 'Errors', [{ Name: 'FunctionName', Value: name }]),
        _sumMetric('AWS/Lambda', 'Invocations', [{ Name: 'FunctionName', Value: name }]),
      ]);
      if (invocations > 0 && (errors / invocations) * 100 >= HEALTH_THRESHOLDS.lambdaErrorDegradedPct) degraded++;
    } catch {
      // Same reasoning as Step Functions above — can't confirm, don't penalize.
    }
  }
  let status = 'healthy';
  if (down > 0) status = 'down';
  else if (degraded > 0) status = 'degraded';
  const total = HEALTH_LAMBDA_FUNCTION_NAMES.length;
  const detailParts = [];
  if (down > 0) detailParts.push(`${down} of ${total} not active`);
  if (degraded > 0) detailParts.push(`${degraded} of ${total} elevated errors`);
  return {
    service: 'lambda',
    label: 'Lambda Functions',
    status,
    detail: detailParts.length ? detailParts.join(' · ') : `${total} of ${total} healthy`,
  };
}

async function checkDynamoDbHealth() {
  const tables = [CONFIG_TABLE, TABLE];
  let down = 0;
  let degraded = 0;
  for (const tableName of tables) {
    try {
      const desc = await dynamodbClient.send(new DescribeTableCommand({ TableName: tableName }));
      if (desc.Table?.TableStatus !== 'ACTIVE') { down++; continue; }
    } catch {
      down++;
      continue;
    }
    try {
      const throttled = await _sumMetric('AWS/DynamoDB', 'ThrottledRequests', [{ Name: 'TableName', Value: tableName }]);
      if (throttled > 0) degraded++;
    } catch {
      // Can't confirm — don't penalize.
    }
  }
  let status = 'healthy';
  if (down > 0) status = 'down';
  else if (degraded > 0) status = 'degraded';
  const detailParts = [];
  if (down > 0) detailParts.push(`${down} of ${tables.length} tables unavailable`);
  if (degraded > 0) detailParts.push(`${degraded} of ${tables.length} throttled`);
  return {
    service: 'dynamodb',
    label: 'DynamoDB',
    status,
    detail: detailParts.length ? detailParts.join(' · ') : `${tables.length} of ${tables.length} healthy`,
  };
}

/** Sums a CloudWatch metric over the last hour. Returns 0 (not an error)
 *  if the metric has no data points yet — that's a normal, common case
 *  (e.g. a function with no recent invocations), not a failure. */
async function _sumMetric(namespace, metricName, dimensions) {
  const end = new Date();
  const start = new Date(end.getTime() - 60 * 60 * 1000);
  const resp = await cloudwatch.send(new GetMetricStatisticsCommand({
    Namespace: namespace,
    MetricName: metricName,
    Dimensions: dimensions,
    StartTime: start,
    EndTime: end,
    Period: 3600,
    Statistics: ['Sum'],
  }));
  return (resp.Datapoints || []).reduce((sum, dp) => sum + (dp.Sum || 0), 0);
}

app.get('/api/dashboard/service-health', async (_req, res) => {
  const checks = [
    { name: 'connectors', fn: checkConnectorsHealth },
    { name: 'queue', fn: checkQueueHealth },
    { name: 'stepFunctions', fn: checkStepFunctionsHealth },
    { name: 'lambda', fn: checkLambdaHealth },
    { name: 'dynamodb', fn: checkDynamoDbHealth },
  ];
  const rows = await Promise.all(checks.map(async ({ name, fn }) => {
    try {
      return await fn();
    } catch (e) {
      console.error(`Service health check '${name}' failed:`, e);
      return { service: name, label: name, status: 'unknown', detail: `Check failed: ${e.message}` };
    }
  }));
  res.json({ rows, checkedAt: nowIso() });
});

// -----------------------------------------------------------------------
// Batches
// -----------------------------------------------------------------------
app.get('/api/batches', async (req, res) => {
    try {
      const application = req.query.application;
      if (!application) {
        return jsonError(res, 400, 'BadRequest', 'application query param is required');
      }
   
      const statusParam = (req.query.status || '').trim();
      const search = (req.query.search || '').trim();
      const page = Math.max(1, safeNumber(req.query.page, 1));
      const pageSize = Math.min(200, Math.max(1, safeNumber(req.query.pageSize, 50)));
      const sortBy = req.query.sortBy || 'jobStartTime';
      const sortOrder = (req.query.sortOrder || 'desc').toLowerCase() === 'asc' ? 'asc' : 'desc';
   
      // Fetch by either status or application
      //
      // Issue 1 (filter chip rename): expand Hold/Pending case-insensitively
      // to cover both canonical and legacy values during the migration window.
      //   "hold"     -> GSI STATUS#hold AND STATUS#OnHold,
      //                 post-filter to OnHold rows whose holdSource === 'manual'
      //   "pending"  -> GSI STATUS#pending AND STATUS#OnHold,
      //                 post-filter to OnHold rows whose holdSource !== 'manual'
      //   else       -> exact GSI query, no post-filter
      let items;
      if (statusParam) {
        const requested = statusParam.split(',').map(s => s.trim()).filter(Boolean);
        const all = [];
        const seen = new Set();  // dedupe across overlapping queries
        for (const raw of requested) {
          const r = raw.toLowerCase();
          let queries;
          let postFilter;
          if (r === 'hold') {
            queries = ['hold', 'OnHold'];
            postFilter = (row) =>
              row.status === 'hold' ||
              (row.status === 'OnHold' && row.holdSource === 'manual');
          } else if (r === 'pending') {
            queries = ['pending', 'OnHold'];
            postFilter = (row) =>
              row.status === 'pending' ||
              (row.status === 'OnHold' && row.holdSource !== 'manual');
          } else {
            queries = [raw];   // preserve original casing for exact match
            postFilter = () => true;
          }
          for (const q of queries) {
            const rows = await queryByStatus(q, 1000);
            for (const row of rows) {
              if (row.application !== application) continue;
              if (!postFilter(row)) continue;
              const key = `${row.queueId}|${row.batchId}`;
              if (seen.has(key)) continue;
              seen.add(key);
              all.push(row);
            }
          }
        }
        items = all;
      } else {
        items = await queryByApp(application, 2000);
      }
   
      // Wildcard filter
      if (search) {
        const rx = wildcardToRegex(search);
        items = items.filter(b =>
          (b.batchId && rx.test(b.batchId)) ||
          (b.batchNumber && rx.test(b.batchNumber))
        );
      }
   
      // Sort
      const sortKeyOf = (it) => {
        if (sortBy === 'queueId') return Number(it.queueId || 0);
        if (sortBy === 'batchNumber') return String(it.batchNumber || '');
        if (sortBy === 'updatedAt') return String(it.updatedAt || '');
        return String(it.jobStartTime || '');
      };
      items.sort((a, b) => {
        const ka = sortKeyOf(a), kb = sortKeyOf(b);
        if (ka < kb) return sortOrder === 'asc' ? -1 : 1;
        if (ka > kb) return sortOrder === 'asc' ? 1 : -1;
        return 0;
      });
   
      const totalCount = items.length;
      const totalPages = Math.max(1, Math.ceil(totalCount / pageSize));
      const startIdx = (page - 1) * pageSize;
      const slice = items.slice(startIdx, startIdx + pageSize);
   
      res.json({
        page,
        pageSize,
        totalCount,
        totalPages,
        batches: slice.map(shapeBatchRow),
      });
    } catch (e) {
      console.error('GET /api/batches error:', e);
      jsonError(res, 500, 'InternalError', e.message);
    }
  });
   
  app.get('/api/batches/:queueId', async (req, res) => {
    try {
      const queueId = safeNumber(req.params.queueId, null);
      if (queueId == null) {
        return jsonError(res, 400, 'BadRequest', 'queueId must be an integer');
      }
      const allItems = await queryBatchAll(queueId);
      if (!allItems.length) {
        return jsonError(res, 404, 'BatchNotFound', `No batch with queueId=${queueId}`);
      }
   
      const meta = allItems.find(i => i.SK === 'META');
      if (!meta) {
        return jsonError(res, 404, 'BatchNotFound', `Batch META missing for queueId=${queueId}`);
      }
   
      const documents = allItems
        .filter(i => i.SK && i.SK.startsWith('DOC#'))
        .map(shapeDocument);
   
      const pages = allItems
        .filter(i => i.SK && i.SK.startsWith('PAGE#'))
        .sort((a, b) => Number(a.pageNumber || 0) - Number(b.pageNumber || 0))
        .map(p => shapePage(p, queueId));
   
      // Attach OCR words to each page in parallel. ocrTextS3Key may be null
      // (pre-OCR) or point at a pre-fix file with no Geometry → empty array.
      await Promise.all(pages.map(async (p) => {
        p.ocrWords = await loadOcrWords(p.ocrTextS3Key);
      }));
   
      const taskHistory = allItems
        .filter(i => i.SK && i.SK.startsWith('TASK#'))
        .sort((a, b) => String(a.endTime || '').localeCompare(String(b.endTime || '')))
        .map(shapeTaskHistory);
   
      const textractJobs = allItems
        .filter(i => i.SK && i.SK.startsWith('TEXTRACTJOB#'))
        .sort((a, b) => String(a.createdAt || '').localeCompare(String(b.createdAt || '')))
        .map(shapeTextractJob);
   
      res.json({
        batch: {
          ...shapeBatchRow(meta),
          sourceZipKey: meta.sourceZipKey || null,
          xmlMetadata: meta.xmlMetadata || {},
          holdSource: meta.holdSource || null,
          holdReason: meta.holdReason || null,
          manualReviewStartedAt: meta.manualReviewStartedAt || null,
          manualReviewCompletedAt: meta.manualReviewCompletedAt || null,
          createdAt: meta.createdAt,
        },
        documents,
        pages,
        taskHistory,
        textractJobs,
      });
    } catch (e) {
      console.error('GET /api/batches/:queueId error:', e);
      jsonError(res, 500, 'InternalError', e.message);
    }
  });
   
  // -----------------------------------------------------------------------
  // Page image (TIFF -> PNG)
  // -----------------------------------------------------------------------
  app.get('/api/batches/:queueId/pages/:pageId/image', async (req, res) => {
    try {
      const queueId = safeNumber(req.params.queueId, null);
      const pageId = req.params.pageId;
      if (queueId == null) {
        return jsonError(res, 400, 'BadRequest', 'queueId must be an integer');
      }
      const page = await ddb.send(new GetCommand({
        TableName: TABLE,
        Key: { PK: `BATCH#${queueId}`, SK: `PAGE#${pageId}` },
      }));
      if (!page.Item) {
        return jsonError(res, 404, 'PageNotFound', `No page ${pageId} for queueId=${queueId}`);
      }
      if (!page.Item.s3Key) {
        return jsonError(res, 500, 'PageMisconfigured', 'Page row has no s3Key');
      }
      const png = await getPageImagePng(page.Item.s3Key);
      res.set('Content-Type', 'image/png');
      res.set('Cache-Control', 'private, max-age=300');
      res.send(png);
    } catch (e) {
      console.error('GET page image error:', e);
      jsonError(res, 500, 'InternalError', e.message);
    }
  });
   
  // -----------------------------------------------------------------------
  // Submit review (resume the Step Function)
  // -----------------------------------------------------------------------
  app.post('/api/batches/:queueId/submit-review', async (req, res) => {
    try {
      const queueId = safeNumber(req.params.queueId, null);
      if (queueId == null) {
        return jsonError(res, 400, 'BadRequest', 'queueId must be an integer');
      }
      const body = req.body || {};
      const reviewer = body.reviewer || getOperator(req) || 'anonymous';
      const corrections = Array.isArray(body.corrections) ? body.corrections : [];
 
      // Stage 5: accept the line items payload. The UI sends a full
      // snapshot of the grid (list of column-name -> value maps), so we
      // overwrite whatever was previously stored rather than merging.
      // Each cell is coerced to string for consistency with the
      // shapeDocument response.
      const incomingLineItems = Array.isArray(body.lineItems) ? body.lineItems : [];
      const lineItemsToStore = incomingLineItems
        .filter(row => row && typeof row === 'object')
        .map(row => {
          const out = {};
          for (const k of Object.keys(row)) {
            const v = row[k];
            out[k] = v == null ? '' : String(v);
          }
          return out;
        });
 
      // Stage 8: tree state + validation override.
      //
      // The UI sends the FINAL desired state for documents + pages
      // (treeDocuments + treePages arrays), not an ops list. We diff
      // against current DDB state and apply the differences via
      // persistTreeState BEFORE field corrections run, so the
      // corrections write hits the post-restructure document records.
      //
      // validationOverride: 'Reviewed' means the user ran Run Validations
      //          and it passed - we promote the document's
      //          validationStatus to 'Reviewed' so the OK state survives
      //          reopens. Otherwise we use the default 'Reviewed' since
      //          submit-review always means user-accepted.
      const treeDocuments = Array.isArray(body.treeDocuments) ? body.treeDocuments : null;
      const treePages     = Array.isArray(body.treePages)     ? body.treePages     : null;
      const validationOverride = body.validationOverride || null;
      let treeSummary = null;
 
      const meta = await getBatchMeta(queueId);
      if (!meta) {
        return jsonError(res, 404, 'BatchNotFound', `No batch with queueId=${queueId}`);
      }
      if (meta.currentTask !== 'ManualReview') {
        return jsonError(res, 409, 'BatchNotInReview',
          `Batch is in currentTask=${meta.currentTask}, not ManualReview`);
      }
 
      // Apply tree state ONLY when the UI sent both arrays. Older clients
      // that don't send tree state still work - we skip persistTreeState
      // entirely. Sending one without the other is an error.
      if (treeDocuments && treePages) {
        try {
          treeSummary = await persistTreeState(queueId, treeDocuments, treePages);
          console.log(`[STAGE8] submit-review persisted tree state:`, JSON.stringify(treeSummary));
        } catch (e) {
          console.error('persistTreeState failed in submit-review:', e);
          return jsonError(res, 500, 'TreeStatePersistFailed', e.message);
        }
      } else if (treeDocuments || treePages) {
        return jsonError(res, 400, 'BadRequest',
          'treeDocuments and treePages must both be sent together');
      }
      const taskToken = meta.manualReviewTaskToken;
      if (!taskToken) {
        return jsonError(res, 409, 'NoTaskToken',
          'Batch has no manualReviewTaskToken; cannot resume');
      }
   
      // Find the document. Phase 1/2: one document per batch.
      const allItems = await queryBatchAll(queueId);
      const documents = allItems.filter(i => i.SK && i.SK.startsWith('DOC#'));
      if (!documents.length) {
        return jsonError(res, 500, 'NoDocument', 'No document found in batch');
      }
 
      // Issue 3: multi-document field state.
      //
      // After Split, a batch can have multiple documents, each with its
      // own field state. The UI sends `documents: [{ documentId,
      // corrections, lineItems }, ...]` payload. Legacy single-doc
      // payload (`corrections` + `lineItems` at root) still works for
      // backward compat — applied only to documents[0].
      const multiDocPayload = Array.isArray(body.documents) ? body.documents : null;
 
      const docUpdates = new Map();   // documentId -> { corrections, lineItems }
      if (multiDocPayload) {
        for (const docEntry of multiDocPayload) {
          if (!docEntry || !docEntry.documentId) continue;
          const corr = Array.isArray(docEntry.corrections) ? docEntry.corrections : [];
          const liRaw = Array.isArray(docEntry.lineItems) ? docEntry.lineItems : [];
          const li = liRaw
            .filter(row => row && typeof row === 'object')
            .map(row => {
              const out = {};
              for (const k of Object.keys(row)) {
                const v = row[k];
                out[k] = v == null ? '' : String(v);
              }
              return out;
            });
          docUpdates.set(docEntry.documentId, { corrections: corr, lineItems: li });
        }
      } else {
        // Legacy: apply to first doc only.
        docUpdates.set('__LEGACY_FIRST_DOC__', {
          corrections,
          lineItems: lineItemsToStore,
        });
      }
 
      // Stage 5: server-side required-field guard. Matches the UI's
      // canSubmit() rule: every required, non-disabled field must have a
      // non-empty value after corrections are applied.
      //
      // CRITICAL: read the Required flag from document-config.json, NOT
      // from the per-batch DDB extractedFields. The ingestion Lambda
      // historically wrote `required: true` for every field regardless
      // of config, so the DDB flag is unreliable. Config is the source
      // of truth.
      const cfg = await loadConfig();
      const requiredFieldNames = new Set();
      const appCfg = (cfg.Applications || cfg.applications || [])
        .find(a => (a.ApplicationName || a.applicationName) === meta.application);
      if (appCfg) {
        const docTypes = appCfg.DocumentTypes || appCfg.documentTypes || [];
        for (const dt of docTypes) {
          for (const pt of (dt.PageTypes || [])) {
            if (pt.PageType !== 'MainPage') continue;
            for (const f of (pt.Fields || [])) {
              if (f.Required === true && f.Disabled !== true) {
                requiredFieldNames.add(f.FieldName);
              }
            }
          }
        }
      }
 
      // Build write plan + validate ALL docs before any write so a
      // partial failure doesn't leave the batch half-corrected.
      const writePlan = [];   // [{ docRec, newFields, lineItemsToStore }]
      const allMissing = [];  // [{ documentId, missing: [fieldName] }]
      for (const docRec of documents) {
        let entry;
        if (multiDocPayload) {
          entry = docUpdates.get(docRec.documentId);
          if (!entry) continue;  // no payload for this doc → skip (no-op)
        } else {
          entry = docUpdates.get('__LEGACY_FIRST_DOC__');
          if (docRec !== documents[0]) continue;
        }
 
        const correctionMap = {};
        for (const c of entry.corrections) {
          if (c && c.fieldName) correctionMap[c.fieldName] = c;
        }
        const currentFields = docRec.extractedFields || [];
        const newFields = currentFields.map(fld => {
          const updated = { ...fld };
          const c = correctionMap[fld.fieldName];
          if (c) {
            updated.value = c.value;
            updated.confidence = 100.0;
            updated.validationPassed = true;
            updated.correctedBy = reviewer;
            updated.correctedAt = nowIso();
            delete updated.validationError;
          }
          return updated;
        });
        // Split docs start with extractedFields=[] — corrections introduce
        // new fields rather than updating existing ones.
        const existingNames = new Set(newFields.map(f => f.fieldName));
        for (const c of entry.corrections) {
          if (c && c.fieldName && !existingNames.has(c.fieldName)) {
            newFields.push({
              fieldName: c.fieldName,
              value: c.value,
              confidence: 100.0,
              validationPassed: true,
              correctedBy: reviewer,
              correctedAt: nowIso(),
              sourcePageId: null,
              required: requiredFieldNames.has(c.fieldName),
              dataType: 'String',
              validationError: null,
            });
          }
        }
 
        // Per-doc required-field guard.
        const missing = newFields
          .filter(f => requiredFieldNames.has(f.fieldName))
          .filter(f => f.value == null || String(f.value).trim() === '')
          .map(f => f.fieldName);
        // Also catch the case where a required field is missing entirely
        // (split docs may have empty extractedFields).
        for (const reqName of requiredFieldNames) {
          if (!newFields.some(f => f.fieldName === reqName)) {
            missing.push(reqName);
          }
        }
        if (missing.length > 0) {
          allMissing.push({ documentId: docRec.documentId, missing: Array.from(new Set(missing)) });
        }
        writePlan.push({ docRec, newFields, lineItemsToStore: entry.lineItems });
      }
 
      if (allMissing.length > 0) {
        const desc = allMissing
          .map(m => `doc ${m.documentId}: ${m.missing.join(', ')}`)
          .join('; ');
        return jsonError(res, 400, 'RequiredFieldsMissing',
          `Required fields are empty: ${desc}`);
      }
 
      // Sequential writes per doc (Stage 8 design: no transactions, cheaper).
      for (const w of writePlan) {
        await ddb.send(new UpdateCommand({
          TableName: TABLE,
          Key: { PK: `BATCH#${queueId}`, SK: w.docRec.SK },
          UpdateExpression: 'SET extractedFields = :ef, lineItems = :li, validationStatus = :vs, updatedAt = :now',
          ExpressionAttributeValues: {
            ':ef': w.newFields,
            ':li': w.lineItemsToStore,
            ':vs': 'Reviewed',
            ':now': nowIso(),
          },
        }));
      }
   
      // Write task history for the review
      const reviewTime = nowIso();
      await ddb.send(new UpdateCommand({
        TableName: TABLE,
        Key: {
          PK: `BATCH#${queueId}`,
          SK: `TASK#${reviewTime}#ManualReview`,
        },
        UpdateExpression: 'SET #t = :t, taskName = :tn, #s = :s, startTime = :st, endTime = :et, #op = :op, queueId = :qid',
        ExpressionAttributeNames: { '#t': 'Type', '#s': 'status', '#op': 'operator' },
        ExpressionAttributeValues: {
          ':t': 'TASKHIST',
          ':tn': 'ManualReview',
          ':s': 'Completed',
          ':st': meta.manualReviewStartedAt || reviewTime,
          ':et': reviewTime,
          ':op': reviewer,
          ':qid': queueId,
        },
      }));
   
      // Clear the stashed token + record completion timestamp
      await ddb.send(new UpdateCommand({
        TableName: TABLE,
        Key: { PK: `BATCH#${queueId}`, SK: 'META' },
        UpdateExpression: 'REMOVE manualReviewTaskToken SET manualReviewCompletedAt = :now, updatedAt = :now',
        ExpressionAttributeValues: { ':now': reviewTime },
      }));
   
      // Resume the Step Function
      await sfn.send(new SendTaskSuccessCommand({
        taskToken,
        output: JSON.stringify({
          queueId,
          application: meta.application,
          reviewer,
          correctionCount: corrections.length,
        }),
      }));
   
      console.log(`✓ submit-review queueId=${queueId} by ${reviewer} (${corrections.length} corrections, ${lineItemsToStore.length} line items)`);
      res.json({
        queueId,
        status: 'Resumed',
        reviewer,
        correctionCount: corrections.length,
        lineItemCount: lineItemsToStore.length,
        treeSummary,  // null if no tree state was sent; otherwise the diff counts
        message: 'Manual review submitted; pipeline resumed to Export',
      });
    } catch (e) {
      console.error('POST /api/batches/:queueId/submit-review error:', e);
      jsonError(res, 500, 'InternalError', e.message);
    }
  });
 
  // -----------------------------------------------------------------------
  // Save draft (Hold from Verify screen) — persist work without resuming
  // -----------------------------------------------------------------------
  // Same payload shape as submit-review (reviewer, corrections, lineItems)
  // but with three key differences:
  //   1. Does NOT call SendTaskSuccess - the Step Function stays paused.
  //   2. Does NOT clear manualReviewTaskToken - batch stays parked.
  //   3. Does NOT enforce required-field validation - lets users save
  //      partial work with empty required fields.
  // Result: batch stays in ManualReview/OnHold; reopening Verify shows
  // the saved values; CIF auto-lookup is skipped because line items are
  // already populated.
app.post('/api/batches/:queueId/save-draft', async (req, res) => {
    try {
      const queueId = safeNumber(req.params.queueId, null);
      if (queueId == null) {
        return jsonError(res, 400, 'BadRequest', 'queueId must be an integer');
      }
      const body = req.body || {};
      const reviewer = body.reviewer || getOperator(req) || 'anonymous';
      const corrections = Array.isArray(body.corrections) ? body.corrections : [];
 
      // Same defensive coercion as submit-review.
      const incomingLineItems = Array.isArray(body.lineItems) ? body.lineItems : [];
      const lineItemsToStore = incomingLineItems
        .filter(row => row && typeof row === 'object')
        .map(row => {
          const out = {};
          for (const k of Object.keys(row)) {
            const v = row[k];
            out[k] = v == null ? '' : String(v);
          }
          return out;
        });
 
      // Stage 8: tree state + validation override (same shape as submit-review).
      const treeDocuments = Array.isArray(body.treeDocuments) ? body.treeDocuments : null;
      const treePages     = Array.isArray(body.treePages)     ? body.treePages     : null;
      const validationOverride = body.validationOverride || null;
      let treeSummary = null;
 
      const meta = await getBatchMeta(queueId);
      if (!meta) {
        return jsonError(res, 404, 'BatchNotFound', `No batch with queueId=${queueId}`);
      }
      // Hold only makes sense when the batch is parked in ManualReview.
      // If a caller hits this for a Running or completed batch, reject
      // rather than corrupt state silently.
      if (meta.currentTask !== 'ManualReview') {
        return jsonError(res, 409, 'BatchNotInReview',
          `Batch is in currentTask=${meta.currentTask}, not ManualReview`);
      }
 
      // Apply tree state BEFORE we re-query documents below, so the
      // subsequent corrections write hits the post-restructure records.
      if (treeDocuments && treePages) {
        try {
          treeSummary = await persistTreeState(queueId, treeDocuments, treePages);
          console.log(`[STAGE8] save-draft persisted tree state:`, JSON.stringify(treeSummary));
        } catch (e) {
          console.error('persistTreeState failed in save-draft:', e);
          return jsonError(res, 500, 'TreeStatePersistFailed', e.message);
        }
      } else if (treeDocuments || treePages) {
        return jsonError(res, 400, 'BadRequest',
          'treeDocuments and treePages must both be sent together');
      }
 
      const allItems = await queryBatchAll(queueId);
      const documents = allItems.filter(i => i.SK && i.SK.startsWith('DOC#'));
      if (!documents.length) {
        return jsonError(res, 500, 'NoDocument', 'No document found in batch');
      }
 
      // Issue 3: multi-document field state (same pattern as submit-review).
      // save-draft does NOT enforce required-field validation — partial saves.
      const persistedStatus = (validationOverride === 'Reviewed') ? 'Reviewed' : 'Draft';
      const multiDocPayload = Array.isArray(body.documents) ? body.documents : null;
 
      const docUpdates = new Map();
      if (multiDocPayload) {
        for (const docEntry of multiDocPayload) {
          if (!docEntry || !docEntry.documentId) continue;
          const corr = Array.isArray(docEntry.corrections) ? docEntry.corrections : [];
          const liRaw = Array.isArray(docEntry.lineItems) ? docEntry.lineItems : [];
          const li = liRaw
            .filter(row => row && typeof row === 'object')
            .map(row => {
              const out = {};
              for (const k of Object.keys(row)) {
                const v = row[k];
                out[k] = v == null ? '' : String(v);
              }
              return out;
            });
          docUpdates.set(docEntry.documentId, { corrections: corr, lineItems: li });
        }
      } else {
        docUpdates.set('__LEGACY_FIRST_DOC__', {
          corrections,
          lineItems: lineItemsToStore,
        });
      }
 
      // Per-doc apply + write.
      for (const docRec of documents) {
        let entry;
        if (multiDocPayload) {
          entry = docUpdates.get(docRec.documentId);
          if (!entry) continue;
        } else {
          entry = docUpdates.get('__LEGACY_FIRST_DOC__');
          if (docRec !== documents[0]) continue;
        }
 
        const correctionMap = {};
        for (const c of entry.corrections) {
          if (c && c.fieldName) correctionMap[c.fieldName] = c;
        }
        const currentFields = docRec.extractedFields || [];
        const newFields = currentFields.map(fld => {
          const updated = { ...fld };
          const c = correctionMap[fld.fieldName];
          if (c) {
            // save-draft semantics: don't bump confidence, don't mark
            // validationPassed; just record the value + audit.
            updated.value = c.value;
            updated.correctedBy = reviewer;
            updated.correctedAt = nowIso();
          }
          return updated;
        });
        // Split docs start with extractedFields=[] — append any correction
        // whose fieldName isn't already in newFields.
        const existingNames = new Set(newFields.map(f => f.fieldName));
        for (const c of entry.corrections) {
          if (c && c.fieldName && !existingNames.has(c.fieldName)) {
            newFields.push({
              fieldName: c.fieldName,
              value: c.value,
              confidence: null,
              validationPassed: false,
              correctedBy: reviewer,
              correctedAt: nowIso(),
              sourcePageId: null,
              required: false,
              dataType: 'String',
              validationError: null,
            });
          }
        }
 
        await ddb.send(new UpdateCommand({
          TableName: TABLE,
          Key: { PK: `BATCH#${queueId}`, SK: docRec.SK },
          UpdateExpression: 'SET extractedFields = :ef, lineItems = :li, validationStatus = :vs, updatedAt = :now',
          ExpressionAttributeValues: {
            ':ef': newFields,
            ':li': entry.lineItems,
            ':vs': persistedStatus,
            ':now': nowIso(),
          },
        }));
      }
 
      // Drop a TASKHIST row so the audit trail shows the save happened.
      const ts = nowIso();
      // Issue 2: when the UI Hold button calls save-draft, body.holdAction=true.
      // We persist corrections/lineItems/tree state as usual, then on the META
      // touch we ALSO flip status from 'pending' (or whatever) to 'hold' with
      // holdSource='manual'. previousStatus is stashed so /release can restore
      // it. The Step Function task token stays parked either way — a later
      // submit-review or /release-then-submit will resume the same execution.
      const holdAction = body.holdAction === true;
      const holdReason = body.holdReason || '';
      await ddb.send(new UpdateCommand({
        TableName: TABLE,
        Key: {
          PK: `BATCH#${queueId}`,
          SK: `TASK#${ts}#ManualReview`,
        },
        UpdateExpression: 'SET #t = :t, taskName = :tn, #s = :s, startTime = :st, endTime = :et, #op = :op, queueId = :qid',
        ExpressionAttributeNames: { '#t': 'Type', '#s': 'status', '#op': 'operator' },
        ExpressionAttributeValues: {
          ':t': 'TASKHIST',
          ':tn': 'ManualReview',
          ':s': holdAction ? 'HeldByUser' : 'Saved',
          ':st': ts,
          ':et': ts,
          ':op': reviewer,
          ':qid': queueId,
        },
      }));
 
      // Touch META. If holdAction, also flip status + holdSource.
      if (holdAction) {
        // Stash previousStatus so /release can restore. Idempotent re-hold
        // safety: if already on hold, keep the original previousStatus.
        const prev = (meta.status === 'hold')
          ? (meta.previousStatus || 'pending')
          : (meta.status || 'pending');
        await ddb.send(new UpdateCommand({
          TableName: TABLE,
          Key: { PK: `BATCH#${queueId}`, SK: 'META' },
          UpdateExpression:
            'SET #s = :s, GSI1PK = :gsi, holdSource = :hs, holdReason = :hr, ' +
            'previousStatus = :ps, heldBy = :op, heldAt = :now, updatedAt = :now',
          ExpressionAttributeNames: { '#s': 'status' },
          ExpressionAttributeValues: {
            ':s':   'hold',
            ':gsi': 'STATUS#hold',
            ':hs':  'manual',
            ':hr':  holdReason,
            ':ps':  prev,
            ':op':  reviewer,
            ':now': ts,
          },
        }));
      } else {
        await ddb.send(new UpdateCommand({
          TableName: TABLE,
          Key: { PK: `BATCH#${queueId}`, SK: 'META' },
          UpdateExpression: 'SET updatedAt = :now',
          ExpressionAttributeValues: { ':now': ts },
        }));
      }
 
      console.log(`✓ save-draft queueId=${queueId} by ${reviewer} (${corrections.length} corrections, ${lineItemsToStore.length} line items)${holdAction ? ' [HoldAction]' : ''}`);
      res.json({
        queueId,
        status: holdAction ? 'hold' : 'Saved',
        reviewer,
        correctionCount: corrections.length,
        lineItemCount: lineItemsToStore.length,
        treeSummary,
        message: holdAction
          ? 'Batch placed on hold (status=hold). Release via /release or resume via submit-review.'
          : 'Draft saved; batch remains in ManualReview',
      });
    } catch (e) {
      console.error('POST /api/batches/:queueId/save-draft error:', e);
      jsonError(res, 500, 'InternalError', e.message);
    }
  });
   
  // -----------------------------------------------------------------------
  // Manual hold / release (operational status, NOT tied to Step Function)
  // -----------------------------------------------------------------------
  app.post('/api/batches/:queueId/hold', async (req, res) => {
    try {
      const queueId = safeNumber(req.params.queueId, null);
      if (queueId == null) {
        return jsonError(res, 400, 'BadRequest', 'queueId must be an integer');
      }
      const body = req.body || {};
      const operator = body.operator || getOperator(req);
      const reason = body.reason || '';
   
      const meta = await getBatchMeta(queueId);
      if (!meta) {
        return jsonError(res, 404, 'BatchNotFound', `No batch with queueId=${queueId}`);
      }
      if (meta.status === 'hold' || meta.status === 'pending' || meta.status === 'OnHold') {
        return jsonError(res, 409, 'AlreadyOnHold',
          `Batch is already in ${meta.status} state`);
      }
      // Don't let a manual hold mask a real review-pending hold
      if (meta.currentTask === 'ManualReview') {
        return jsonError(res, 409, 'BatchInReview',
          'Batch is in ManualReview; use submit-review to resolve');
      }
   
      const previousStatus = meta.status;
      const updateTime = nowIso();
      await ddb.send(new UpdateCommand({
        TableName: TABLE,
        Key: { PK: `BATCH#${queueId}`, SK: 'META' },
        UpdateExpression: 'SET #s = :s, GSI1PK = :gsi1, holdSource = :hs, holdReason = :hr, ' +
                          'previousStatus = :ps, heldBy = :op, heldAt = :now, updatedAt = :now',
        ExpressionAttributeNames: { '#s': 'status' },
        ExpressionAttributeValues: {
          ':s': 'hold',
          ':gsi1': 'STATUS#hold',
          ':hs': 'manual',
          ':hr': reason,
          ':ps': previousStatus,
          ':op': operator,
          ':now': updateTime,
        },
      }));
      res.json({ queueId, status: 'hold', heldBy: operator, reason });
    } catch (e) {
      console.error('POST /api/batches/:queueId/hold error:', e);
      jsonError(res, 500, 'InternalError', e.message);
    }
  });
   
  app.post('/api/batches/:queueId/release', async (req, res) => {
    try {
      const queueId = safeNumber(req.params.queueId, null);
      if (queueId == null) {
        return jsonError(res, 400, 'BadRequest', 'queueId must be an integer');
      }
      const body = req.body || {};
      const operator = body.operator || getOperator(req);
   
      const meta = await getBatchMeta(queueId);
      if (!meta) {
        return jsonError(res, 404, 'BatchNotFound', `No batch with queueId=${queueId}`);
      }
      // Release path: only valid for user-initiated holds (status="hold"),
      // and for legacy OnHold rows that came from the manual-hold path
      // (holdSource="manual"). System-routed "pending" batches must be
      // resolved via submit-review, which resumes the Step Function.
      const isUserHold =
        meta.status === 'hold' ||
        (meta.status === 'OnHold' && meta.holdSource === 'manual');
      if (!isUserHold) {
        if (meta.status === 'pending' || meta.currentTask === 'ManualReview') {
          return jsonError(res, 409, 'NotManualHold',
            'Batch is in system-routed ManualReview; use submit-review');
        }
        return jsonError(res, 409, 'NotOnHold',
          `Batch is in status=${meta.status}, not held`);
      }
   
      const restored = meta.previousStatus || 'Running';
      const updateTime = nowIso();
      await ddb.send(new UpdateCommand({
        TableName: TABLE,
        Key: { PK: `BATCH#${queueId}`, SK: 'META' },
        UpdateExpression: 'SET #s = :s, GSI1PK = :gsi1, updatedAt = :now ' +
                          'REMOVE holdSource, holdReason, previousStatus, heldBy, heldAt',
        ExpressionAttributeNames: { '#s': 'status' },
        ExpressionAttributeValues: {
          ':s': restored,
          ':gsi1': `STATUS#${restored}`,
          ':now': updateTime,
        },
      }));
      res.json({ queueId, status: restored, releasedBy: operator });
    } catch (e) {
      console.error('POST /api/batches/:queueId/release error:', e);
      jsonError(res, 500, 'InternalError', e.message);
    }
  });
   
  // ═════════════════════════════════════════════════════════════════════════
  // STUDIO (Akrify) — config authoring API
  //
  // All rows live in the `idp-config` DynamoDB table (env: IDP_CONFIG_TABLE).
  //
  // Schema:
  //   PK              SK             type          body
  //   "APP#<id>"      "META"         APPLICATION   { ApplicationName, DisplayName, ClassificationMode, ... }
  //   "APP#<id>"      "DT#<id>"      DOC_TYPE      { DocumentTypeName, PageTypes: [...] }
  //   "DICT"          "<name>"       DICTIONARY    { options: [ {Value, Label} ] }
  //
  // GSI (env: IDP_CONFIG_TYPE_INDEX, default "type-lastModifiedAt-index"):
  //   PK: type   SK: lastModifiedAt
  //   Powers listApplications() and listAllDocumentTypes() without Scans.
  //
  // Concurrency:
  //   Every PUT/DELETE reads X-Expected-Version header and applies it as a
  //   DDB ConditionExpression. On mismatch → 409 with the current version so
  //   the client can prompt the user to reload.
  //
  // Cache:
  //   Any successful write to this table invalidates the in-memory loadConfig
  //   cache so the reviewer app sees changes on its next call.
  // ═════════════════════════════════════════════════════════════════════════

  // ─── STUDIO: DDB helpers ────────────────────────────────────────────────

  async function ddbConfigQueryByType(type) {
    const items = [];
    let ExclusiveStartKey;
    do {
      const resp = await ddb.send(new QueryCommand({
        TableName: CONFIG_TABLE,
        IndexName: CONFIG_TYPE_INDEX,
        KeyConditionExpression: '#t = :t',
        ExpressionAttributeNames:  { '#t': 'type' },
        ExpressionAttributeValues: { ':t': type },
        ExclusiveStartKey,
      }));
      items.push(...(resp.Items || []));
      ExclusiveStartKey = resp.LastEvaluatedKey;
    } while (ExclusiveStartKey);
    return items;
  }

  async function ddbConfigGet(pk, sk) {
    const resp = await ddb.send(new GetCommand({
      TableName: CONFIG_TABLE,
      Key: { PK: pk, SK: sk },
    }));
    return resp.Item || null;
  }

  async function ddbConfigListDocTypes(appId) {
    const items = [];
    let ExclusiveStartKey;
    do {
      const resp = await ddb.send(new QueryCommand({
        TableName: CONFIG_TABLE,
        KeyConditionExpression: 'PK = :pk AND begins_with(SK, :dt)',
        ExpressionAttributeValues: { ':pk': `APP#${appId}`, ':dt': 'DT#' },
        ExclusiveStartKey,
      }));
      items.push(...(resp.Items || []));
      ExclusiveStartKey = resp.LastEvaluatedKey;
    } while (ExclusiveStartKey);
    return items;
  }

  /**
   * Write a row with optimistic concurrency. If expectedVersion is null,
   * writes unconditionally (create case). Otherwise, applies
   * ConditionExpression "version = :expected" and throws on mismatch.
   * Increments version and stamps timestamps on success.
   */
  async function ddbConfigPutRow(item, expectedVersion) {
    const nextVersion = (expectedVersion == null) ? 1 : expectedVersion + 1;
    const now = nowIso();
    const finalItem = {
      ...item,
      version: nextVersion,
      lastModifiedAt: now,
      lastModifiedBy: STUDIO_USER,
    };
    const params = {
      TableName: CONFIG_TABLE,
      Item: finalItem,
    };
    if (expectedVersion != null) {
      params.ConditionExpression = 'attribute_exists(PK) AND version = :expected';
      params.ExpressionAttributeValues = { ':expected': expectedVersion };
    } else {
      // Creation: fail if row already exists.
      params.ConditionExpression = 'attribute_not_exists(PK)';
    }
    try {
      await ddb.send(new PutCommand(params));
      invalidateConfigCache();
      return finalItem;
    } catch (e) {
      if (e.name === 'ConditionalCheckFailedException') {
        const err = new Error('VersionConflict');
        err.code = 'VersionConflict';
        err.pk = item.PK;
        err.sk = item.SK;
        throw err;
      }
      throw e;
    }
  }

  async function ddbConfigDelete(pk, sk) {
    await ddb.send(new DeleteCommand({
      TableName: CONFIG_TABLE,
      Key: { PK: pk, SK: sk },
    }));
    invalidateConfigCache();
  }

  // ─── STUDIO: response shape helpers ─────────────────────────────────────

  function appIdFromPK(pk)     { return String(pk).replace(/^APP#/, ''); }
  function docTypeIdFromSK(sk) { return String(sk).replace(/^DT#/, ''); }

  function toAppSummary(row, docTypeCount) {
    const appId = appIdFromPK(row.PK);
    const body  = row.body || {};
    return {
      appId,
      name:               body.DisplayName || body.ApplicationName || appId,
      classificationMode: body.ClassificationMode || 'Sequential',
      docTypeCount:       docTypeCount || 0,
      version:            row.version || 1,
      lastModifiedAt:     row.lastModifiedAt,
      lastModifiedBy:     row.lastModifiedBy || STUDIO_USER,
    };
  }

  function toAppRecord(row, docTypeRows) {
    const summary = toAppSummary(row, (docTypeRows || []).length);
    return {
      ...summary,
      documentTypes: (docTypeRows || []).map(toDocTypeSummary),
    };
  }

  function toDocTypeSummary(row) {
    const appId     = appIdFromPK(row.PK);
    const docTypeId = docTypeIdFromSK(row.SK);
    const body      = row.body || {};
    const pageTypes = body.PageTypes || [];
    const fieldCount = pageTypes.reduce((n, pt) => n + ((pt.Fields || []).length), 0);
    return {
      appId,
      docTypeId,
      name:           body.DisplayName || body.DocumentTypeName || docTypeId,
      pageCount:      pageTypes.length,
      fieldCount,
      jobCount:       row.jobCount   ?? 0,
      stpRate:        row.stpRate    ?? null,
      status:         row.status     ?? 'active',
      version:        row.version    || 1,
      lastModifiedAt: row.lastModifiedAt,
      lastModifiedBy: row.lastModifiedBy || STUDIO_USER,
    };
  }

  function toDocTypeRecord(row) {
    return { ...toDocTypeSummary(row), body: row.body || {} };
  }

  function toDictionaryRecord(row) {
    const body = row.body || {};
    return {
      name:           String(row.SK),
      options:        body.options || [],
      version:        row.version || 1,
      lastModifiedAt: row.lastModifiedAt,
      lastModifiedBy: row.lastModifiedBy || STUDIO_USER,
    };
  }

  function parseExpectedVersion(req) {
    const raw = req.get('X-Expected-Version');
    if (raw == null) return null;
    const n = parseInt(raw, 10);
    return Number.isFinite(n) ? n : null;
  }

  function requireExpectedVersion(req, res) {
    const v = parseExpectedVersion(req);
    if (v == null) {
      jsonError(res, 400, 'MissingExpectedVersion',
        'X-Expected-Version header is required for updates.');
      return null;
    }
    return v;
  }

  async function respondVersionConflict(res, kind, id) {
    // On CCF, fetch the current row so the client knows what version it lost to.
    let currentVersion = null;
    if (kind === 'APPLICATION') {
      const row = await ddbConfigGet(`APP#${id}`, 'META');
      currentVersion = row?.version ?? null;
    } else if (kind === 'DOC_TYPE') {
      const [appId, docTypeId] = String(id).split('/');
      const row = await ddbConfigGet(`APP#${appId}`, `DT#${docTypeId}`);
      currentVersion = row?.version ?? null;
    } else if (kind === 'DICTIONARY') {
      const row = await ddbConfigGet('DICT', id);
      currentVersion = row?.version ?? null;
    } else if (kind === 'WORKFLOW') {
      const row = await ddbConfigGet('WORKFLOW', String(id));
      currentVersion = row?.version ?? null;
    } else if (kind === 'CONNECTOR') {
      const row = await ddbConfigGet('CONNECTOR', String(id));
      currentVersion = row?.version ?? null;
    }
    return res.status(409).json({
      error: 'VersionConflict',
      kind,
      id,
      currentVersion,
      message: `${kind} ${id} was modified by someone else` +
               (currentVersion != null ? ` (now at v${currentVersion}).` : '.'),
    });
  }

  // ─── STUDIO: Applications ───────────────────────────────────────────────

  // Flat list for the Studio Applications page. Reviewer app still uses
  // the existing GET /api/applications (nested tree).
  app.get('/api/applications/summary', async (_req, res) => {
    try {
      const [appRows, dtRows] = await Promise.all([
        ddbConfigQueryByType('APPLICATION'),
        ddbConfigQueryByType('DOC_TYPE'),
      ]);
      const countsByApp = {};
      for (const dt of dtRows) {
        const appId = appIdFromPK(dt.PK);
        countsByApp[appId] = (countsByApp[appId] || 0) + 1;
      }
      const applications = appRows.map(r => toAppSummary(r, countsByApp[appIdFromPK(r.PK)] || 0));
      applications.sort((a, b) => a.name.localeCompare(b.name));
      res.json({ applications });
    } catch (e) {
      console.error('GET /api/applications/summary error:', e);
      jsonError(res, 500, 'InternalError', e.message);
    }
  });

  // Single application + its doc type summaries. Used by the Studio editor.
  app.get('/api/applications/:appId', async (req, res) => {
    try {
      const { appId } = req.params;
      const [metaRow, dtRows] = await Promise.all([
        ddbConfigGet(`APP#${appId}`, 'META'),
        ddbConfigListDocTypes(appId),
      ]);
      if (!metaRow) return jsonError(res, 404, 'NotFound', `Application ${appId} not found`);
      res.json(toAppRecord(metaRow, dtRows));
    } catch (e) {
      console.error('GET /api/applications/:appId error:', e);
      jsonError(res, 500, 'InternalError', e.message);
    }
  });

  app.post('/api/applications', async (req, res) => {
    try {
      const { appId, name, classificationMode } = req.body || {};
      if (!appId || !/^[A-Z0-9_]+$/.test(String(appId))) {
        return jsonError(res, 400, 'BadRequest',
          'appId is required and must be uppercase alphanumerics/underscores.');
      }
      const existing = await ddbConfigGet(`APP#${appId}`, 'META');
      if (existing) {
        return jsonError(res, 409, 'AlreadyExists', `Application ${appId} already exists`);
      }
      const item = {
        PK:   `APP#${appId}`,
        SK:   'META',
        type: 'APPLICATION',
        body: {
          ApplicationName:    appId,
          DisplayName:        name || appId,
          ClassificationMode: classificationMode || 'Sequential',
        },
      };
      const written = await ddbConfigPutRow(item, null);
      res.status(201).json(toAppRecord(written, []));
    } catch (e) {
      console.error('POST /api/applications error:', e);
      jsonError(res, 500, 'InternalError', e.message);
    }
  });

  app.put('/api/applications/:appId', async (req, res) => {
    try {
      const { appId } = req.params;
      const expected = requireExpectedVersion(req, res);
      if (expected == null) return;
      const existing = await ddbConfigGet(`APP#${appId}`, 'META');
      if (!existing) return jsonError(res, 404, 'NotFound', `Application ${appId} not found`);

      const body = existing.body || {};
      const { name, classificationMode } = req.body || {};
      const merged = {
        ApplicationName:    body.ApplicationName || appId,
        DisplayName:        name               ?? body.DisplayName,
        ClassificationMode: classificationMode ?? body.ClassificationMode ?? 'Sequential',
      };
      const item = { PK: `APP#${appId}`, SK: 'META', type: 'APPLICATION', body: merged };
      const written = await ddbConfigPutRow(item, expected);
      const dtRows = await ddbConfigListDocTypes(appId);
      res.json(toAppRecord(written, dtRows));
    } catch (e) {
      if (e.code === 'VersionConflict') return respondVersionConflict(res, 'APPLICATION', req.params.appId);
      console.error('PUT /api/applications/:appId error:', e);
      jsonError(res, 500, 'InternalError', e.message);
    }
  });

  // Cascade: deleting an application also deletes all its doc type rows.
  app.delete('/api/applications/:appId', async (req, res) => {
    try {
      const { appId } = req.params;
      const dtRows = await ddbConfigListDocTypes(appId);
      for (const dt of dtRows) {
        await ddbConfigDelete(dt.PK, dt.SK);
      }
      await ddbConfigDelete(`APP#${appId}`, 'META');

      // Workflows aren't owned by this application (they're shared,
      // mappable to any number of apps) — deleting the app doesn't delete
      // them, it just un-maps this appId so nothing dangling is left behind.
      // Best-effort: the app is already deleted by this point, so a rare
      // version conflict here (someone editing that workflow at this exact
      // moment) shouldn't fail the whole request — just log it. Studio's
      // "Manage Applications" modal can clean up the stale reference
      // manually if this happens.
      try {
        const workflowRows = await ddbConfigListAllWorkflows();
        for (const row of workflowRows) {
          const mapped = row.body?.mappedApplicationIds || [];
          if (mapped.includes(appId)) {
            const item = {
              PK: row.PK,
              SK: row.SK,
              type: 'WORKFLOW',
              body: { ...row.body, mappedApplicationIds: mapped.filter(id => id !== appId) },
            };
            await ddbConfigPutRow(item, row.version);
          }
        }
      } catch (cleanupError) {
        console.warn(`Non-fatal: failed to unmap ${appId} from one or more workflows:`, cleanupError);
      }

      res.status(204).end();
    } catch (e) {
      console.error('DELETE /api/applications/:appId error:', e);
      jsonError(res, 500, 'InternalError', e.message);
    }
  });

  // ─── STUDIO: Document Types ─────────────────────────────────────────────

  // Global list — used by the /document-types Studio route.
  app.get('/api/document-types', async (_req, res) => {
    try {
      const rows = await ddbConfigQueryByType('DOC_TYPE');
      const documentTypes = rows.map(toDocTypeSummary);
      documentTypes.sort((a, b) => a.name.localeCompare(b.name));
      res.json({ documentTypes });
    } catch (e) {
      console.error('GET /api/document-types error:', e);
      jsonError(res, 500, 'InternalError', e.message);
    }
  });

  app.get('/api/applications/:appId/document-types', async (req, res) => {
    try {
      const { appId } = req.params;
      const rows = await ddbConfigListDocTypes(appId);
      const documentTypes = rows.map(toDocTypeSummary);
      documentTypes.sort((a, b) => a.name.localeCompare(b.name));
      res.json({ documentTypes });
    } catch (e) {
      console.error('GET /api/applications/:appId/document-types error:', e);
      jsonError(res, 500, 'InternalError', e.message);
    }
  });

  app.get('/api/applications/:appId/document-types/:docTypeId', async (req, res) => {
    try {
      const { appId, docTypeId } = req.params;
      const row = await ddbConfigGet(`APP#${appId}`, `DT#${docTypeId}`);
      if (!row) return jsonError(res, 404, 'NotFound', `Document type ${appId}/${docTypeId} not found`);
      res.json(toDocTypeRecord(row));
    } catch (e) {
      console.error('GET /api/applications/:appId/document-types/:docTypeId error:', e);
      jsonError(res, 500, 'InternalError', e.message);
    }
  });

  /**
   * Default page scaffold for a brand-new document type: MainPage,
   * TrailingPage, BlankPage. BlankPage always uses WordCount (word-count
   * thresholds are how blank pages get identified regardless of how the
   * rest of the document type classifies its pages). MainPage/TrailingPage
   * follow classificationMode — Sequential (position-based, gets a
   * SequenceOrder) or Keyword (OCR text matching, starts with an empty
   * Keywords list for the person to fill in). These are just SEED
   * defaults for pages created at this moment — changing ClassificationMode
   * later via the Classification tab does not retroactively touch these
   * or any other page's IdentificationMethod.
   */
  function buildDocumentTypeScaffold(docTypeId, name, classificationMode, blankPageMaxWordCount) {
    const mode = classificationMode === 'Keyword' ? 'Keyword' : 'Sequential';
    const blankMax = Number.isFinite(blankPageMaxWordCount) ? blankPageMaxWordCount : 5;

    const nonBlankPage = (pageType, sequenceOrder) =>
      mode === 'Sequential'
        ? { PageType: pageType, IdentificationMethod: 'Sequential', SequenceOrder: sequenceOrder, Fields: [] }
        : { PageType: pageType, IdentificationMethod: 'Keyword', Keywords: [], Fields: [] };

    return {
      DocumentTypeName: docTypeId,
      DisplayName:      name || docTypeId,
      ClassificationMode: mode,
      BlankPageMaxWordCount: blankMax,
      PageTypes: [
        nonBlankPage('MainPage', 1),
        nonBlankPage('TrailingPage', 2),
        { PageType: 'BlankPage', IdentificationMethod: 'WordCount', MaxWordCount: blankMax },
      ],
    };
  }

  app.post('/api/applications/:appId/document-types', async (req, res) => {
    try {
      const { appId } = req.params;
      const { docTypeId, name, body, classificationMode, blankPageMaxWordCount } = req.body || {};
      if (!docTypeId || !/^[A-Z0-9_]+$/.test(String(docTypeId))) {
        return jsonError(res, 400, 'BadRequest',
          'docTypeId is required and must be uppercase alphanumerics/underscores.');
      }
      const app = await ddbConfigGet(`APP#${appId}`, 'META');
      if (!app) return jsonError(res, 404, 'NotFound', `Application ${appId} not found`);

      const existing = await ddbConfigGet(`APP#${appId}`, `DT#${docTypeId}`);
      if (existing) return jsonError(res, 409, 'AlreadyExists',
        `Document type ${docTypeId} already exists in ${appId}`);

      const dtBody = body || buildDocumentTypeScaffold(docTypeId, name, classificationMode, blankPageMaxWordCount);
      const item = {
        PK:     `APP#${appId}`,
        SK:     `DT#${docTypeId}`,
        type:   'DOC_TYPE',
        body:   dtBody,
        status: 'draft',
      };
      const written = await ddbConfigPutRow(item, null);
      res.status(201).json(toDocTypeRecord(written));
    } catch (e) {
      console.error('POST /api/applications/:appId/document-types error:', e);
      jsonError(res, 500, 'InternalError', e.message);
    }
  });

  app.put('/api/applications/:appId/document-types/:docTypeId', async (req, res) => {
    try {
      const { appId, docTypeId } = req.params;
      const expected = requireExpectedVersion(req, res);
      if (expected == null) return;
      const existing = await ddbConfigGet(`APP#${appId}`, `DT#${docTypeId}`);
      if (!existing) return jsonError(res, 404, 'NotFound', `Document type ${appId}/${docTypeId} not found`);

      const { body, status } = req.body || {};
      const item = {
        PK:     `APP#${appId}`,
        SK:     `DT#${docTypeId}`,
        type:   'DOC_TYPE',
        body:   body   || existing.body,
        status: status || existing.status || 'active',
      };
      const written = await ddbConfigPutRow(item, expected);
      res.json(toDocTypeRecord(written));
    } catch (e) {
      if (e.code === 'VersionConflict') {
        return respondVersionConflict(res, 'DOC_TYPE', `${req.params.appId}/${req.params.docTypeId}`);
      }
      console.error('PUT /api/applications/:appId/document-types/:docTypeId error:', e);
      jsonError(res, 500, 'InternalError', e.message);
    }
  });

  app.delete('/api/applications/:appId/document-types/:docTypeId', async (req, res) => {
    try {
      const { appId, docTypeId } = req.params;
      await ddbConfigDelete(`APP#${appId}`, `DT#${docTypeId}`);
      res.status(204).end();
    } catch (e) {
      console.error('DELETE /api/applications/:appId/document-types/:docTypeId error:', e);
      jsonError(res, 500, 'InternalError', e.message);
    }
  });

  // ─── STUDIO: Dictionaries ───────────────────────────────────────────────

  app.get('/api/dictionaries', async (_req, res) => {
    try {
      const rows = await ddbConfigQueryByType('DICTIONARY');
      const dictionaries = rows.map(toDictionaryRecord);
      dictionaries.sort((a, b) => a.name.localeCompare(b.name));
      res.json({ dictionaries });
    } catch (e) {
      console.error('GET /api/dictionaries error:', e);
      jsonError(res, 500, 'InternalError', e.message);
    }
  });

  app.get('/api/dictionaries/:name', async (req, res) => {
    try {
      const row = await ddbConfigGet('DICT', req.params.name);
      if (!row) return jsonError(res, 404, 'NotFound', `Dictionary ${req.params.name} not found`);
      res.json(toDictionaryRecord(row));
    } catch (e) {
      console.error('GET /api/dictionaries/:name error:', e);
      jsonError(res, 500, 'InternalError', e.message);
    }
  });

  app.put('/api/dictionaries/:name', async (req, res) => {
    try {
      const { name } = req.params;
      const { options } = req.body || {};
      if (!Array.isArray(options)) {
        return jsonError(res, 400, 'BadRequest', 'options must be an array of {Value, Label}');
      }
      const existing = await ddbConfigGet('DICT', name);
      const expected = existing ? parseExpectedVersion(req) : null;
      if (existing && expected == null) {
        return jsonError(res, 400, 'MissingExpectedVersion',
          'X-Expected-Version required when updating an existing dictionary.');
      }
      const item = {
        PK:   'DICT',
        SK:   name,
        type: 'DICTIONARY',
        body: { options },
      };
      const written = await ddbConfigPutRow(item, expected);
      res.json(toDictionaryRecord(written));
    } catch (e) {
      if (e.code === 'VersionConflict') return respondVersionConflict(res, 'DICTIONARY', req.params.name);
      console.error('PUT /api/dictionaries/:name error:', e);
      jsonError(res, 500, 'InternalError', e.message);
    }
  });

  // ─── STUDIO: S3 browsing (backs the Connectors bucket/folder picker) ────
  //
  // Read-only except the explicit "create folder" endpoint. S3 has no real
  // folder concept — "creating a folder" is the same convention the AWS
  // Console itself uses: a zero-byte object whose key ends in "/".

  app.get('/api/s3/buckets', async (_req, res) => {
    try {
      const resp = await s3.send(new ListBucketsCommand({}));
      const buckets = (resp.Buckets || [])
        .map(b => ({ name: b.Name }))
        .sort((a, b) => a.name.localeCompare(b.name));
      res.json({ buckets });
    } catch (e) {
      console.error('GET /api/s3/buckets error:', e);
      jsonError(res, 500, 'InternalError',
        `Failed to list S3 buckets: ${e.message}. Verify the proxy Lambda's role has s3:ListAllMyBuckets.`);
    }
  });

  app.get('/api/s3/buckets/:bucket/folders', async (req, res) => {
    try {
      const { bucket } = req.params;
      const prefix = req.query.prefix ? String(req.query.prefix) : '';
      const normalizedPrefix = prefix && !prefix.endsWith('/') ? `${prefix}/` : prefix;
      const resp = await s3.send(new ListObjectsV2Command({
        Bucket: bucket,
        Prefix: normalizedPrefix,
        Delimiter: '/',
      }));
      const folders = (resp.CommonPrefixes || []).map(cp => {
        const full = cp.Prefix || '';
        const trimmed = full.endsWith('/') ? full.slice(0, -1) : full;
        const name = trimmed.split('/').pop() || trimmed;
        return { prefix: full, name };
      });
      folders.sort((a, b) => a.name.localeCompare(b.name));
      res.json({ folders });
    } catch (e) {
      console.error('GET /api/s3/buckets/:bucket/folders error:', e);
      jsonError(res, 500, 'InternalError',
        `Failed to list folders in bucket "${req.params.bucket}": ${e.message}. ` +
        `Verify the proxy Lambda's role has s3:ListBucket on this bucket.`);
    }
  });

  app.post('/api/s3/buckets/:bucket/folders', async (req, res) => {
    try {
      const { bucket } = req.params;
      const { prefix } = req.body || {};
      if (!prefix || typeof prefix !== 'string') {
        return jsonError(res, 400, 'BadRequest', 'prefix is required.');
      }
      const key = prefix.endsWith('/') ? prefix : `${prefix}/`;
      await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: '' }));
      res.status(201).json({ prefix: key, name: key.slice(0, -1).split('/').pop() || key });
    } catch (e) {
      console.error('POST /api/s3/buckets/:bucket/folders error:', e);
      jsonError(res, 500, 'InternalError',
        `Failed to create folder in bucket "${req.params.bucket}": ${e.message}. ` +
        `Verify the proxy Lambda's role has s3:PutObject on this bucket.`);
    }
  });

  // ─── STUDIO: Connectors ───────────────────────────────────────────────
  //
  // Row shape — each connector is its own independent entity, not one
  // config object per Application (an app can have several — separate S3
  // sources for Fax vs Email intake, for instance):
  //   PK             SK               type        body
  //   "CONNECTOR"    "<connectorId>"  CONNECTOR   { name, direction, platform, appId, status, s3Config? }
  //
  // Flat partition (same pattern Workflows/Dictionaries already use) —
  // listing every connector is a plain Query on PK='CONNECTOR', no GSI
  // needed. Immediately persisted, no draft concept — plain configuration
  // metadata idp-ingestion reads at runtime, not something with a deploy step.

  function toConnectorSummary(row) {
    const body = row?.body || {};
    return {
      connectorId:    row.SK,
      name:           body.name || row.SK,
      direction:      body.direction,
      platform:       body.platform || 'S3',
      appId:          body.appId,
      status:         body.status || 'disabled',
      bucket:         body.s3Config?.bucket,
      prefix:         body.s3Config?.prefix,
      version:        row.version || 1,
      lastModifiedAt: row.lastModifiedAt,
      lastModifiedBy: row.lastModifiedBy || STUDIO_USER,
    };
  }

  function toConnectorRecord(row) {
    const summary = toConnectorSummary(row);
    const body = row?.body || {};
    return { ...summary, s3Config: body.s3Config };
  }

  async function ddbConfigListAllConnectors() {
    const items = [];
    let ExclusiveStartKey;
    do {
      const resp = await ddb.send(new QueryCommand({
        TableName: CONFIG_TABLE,
        KeyConditionExpression: 'PK = :pk',
        ExpressionAttributeValues: { ':pk': 'CONNECTOR' },
        ExclusiveStartKey,
      }));
      items.push(...(resp.Items || []));
      ExclusiveStartKey = resp.LastEvaluatedKey;
    } while (ExclusiveStartKey);
    return items;
  }

  app.get('/api/connectors', async (_req, res) => {
    try {
      const rows = await ddbConfigListAllConnectors();
      const connectors = rows.map(toConnectorSummary);
      connectors.sort((a, b) => a.name.localeCompare(b.name));
      res.json({ connectors });
    } catch (e) {
      console.error('GET /api/connectors error:', e);
      jsonError(res, 500, 'InternalError', e.message);
    }
  });

  app.get('/api/connectors/:connectorId', async (req, res) => {
    try {
      const row = await ddbConfigGet('CONNECTOR', req.params.connectorId);
      if (!row) return jsonError(res, 404, 'NotFound', `Connector ${req.params.connectorId} not found`);
      res.json(toConnectorRecord(row));
    } catch (e) {
      console.error('GET /api/connectors/:connectorId error:', e);
      jsonError(res, 500, 'InternalError', e.message);
    }
  });

  app.post('/api/connectors', async (req, res) => {
    try {
      const { name, direction, platform, appId } = req.body || {};
      if (!name || typeof name !== 'string') {
        return jsonError(res, 400, 'BadRequest', 'name is required.');
      }
      if (direction !== 'import' && direction !== 'export') {
        return jsonError(res, 400, 'BadRequest', "direction must be 'import' or 'export'.");
      }
      if (platform !== 'S3') {
        return jsonError(res, 400, 'BadRequest', "platform must be 'S3' (the only one supported today).");
      }
      if (!appId || typeof appId !== 'string') {
        return jsonError(res, 400, 'BadRequest', 'appId is required.');
      }
      const app = await ddbConfigGet(`APP#${appId}`, 'META');
      if (!app) return jsonError(res, 404, 'NotFound', `Application ${appId} not found`);

      const connectorId = `conn_${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}`;
      const item = {
        PK:   'CONNECTOR',
        SK:   connectorId,
        type: 'CONNECTOR',
        // Starts disabled and unconfigured — nothing for idp-ingestion to
        // route through until a bucket/prefix is set and it's explicitly
        // enabled. No accidental live routing from an incomplete connector.
        body: { name, direction, platform, appId, status: 'disabled' },
      };
      const written = await ddbConfigPutRow(item, null);
      res.status(201).json(toConnectorRecord(written));
    } catch (e) {
      console.error('POST /api/connectors error:', e);
      jsonError(res, 500, 'InternalError', e.message);
    }
  });

  app.put('/api/connectors/:connectorId', async (req, res) => {
    try {
      const { connectorId } = req.params;
      const expected = requireExpectedVersion(req, res);
      if (expected == null) return;

      const existing = await ddbConfigGet('CONNECTOR', connectorId);
      if (!existing) return jsonError(res, 404, 'NotFound', `Connector ${connectorId} not found`);

      const { name, status, s3Config } = req.body || {};
      if (name === undefined && status === undefined && s3Config === undefined) {
        return jsonError(res, 400, 'BadRequest', 'Provide at least one of: name, status, s3Config.');
      }
      if (status !== undefined && status !== 'enabled' && status !== 'disabled') {
        return jsonError(res, 400, 'BadRequest', "status must be 'enabled' or 'disabled'.");
      }

      const body = existing.body || {};
      const item = {
        PK:   'CONNECTOR',
        SK:   connectorId,
        type: 'CONNECTOR',
        body: {
          ...body,
          ...(name !== undefined ? { name } : {}),
          ...(status !== undefined ? { status } : {}),
          ...(s3Config !== undefined ? { s3Config } : {}),
        },
      };
      const written = await ddbConfigPutRow(item, expected);
      res.json(toConnectorRecord(written));
    } catch (e) {
      if (e.code === 'VersionConflict') {
        return respondVersionConflict(res, 'CONNECTOR', req.params.connectorId);
      }
      console.error('PUT /api/connectors/:connectorId error:', e);
      jsonError(res, 500, 'InternalError', e.message);
    }
  });

  app.delete('/api/connectors/:connectorId', async (req, res) => {
    try {
      await ddbConfigDelete('CONNECTOR', req.params.connectorId);
      res.status(204).end();
    } catch (e) {
      console.error('DELETE /api/connectors/:connectorId error:', e);
      jsonError(res, 500, 'InternalError', e.message);
    }
  });

  // Genuinely tests reachability against real S3 — not decorative. A
  // scoped ListObjectsV2 (MaxKeys: 1) against the configured bucket/prefix
  // is enough to prove the bucket exists and this Lambda's role can read
  // it, without needing any object to actually be there yet.
  // Generic — tests reachability against whatever bucket/prefix is passed
  // in, regardless of whether it's been saved anywhere. Deliberately NOT
  // tied to a connectorId: the inspector calls this with its current,
  // possibly-unsaved form values, so Test Connection works the same way
  // whether or not Save has been clicked yet.
  app.post('/api/s3/test', async (req, res) => {
    try {
      const { bucket, prefix } = req.body || {};
      if (!bucket) {
        return res.json({ success: false, message: 'No bucket selected yet.' });
      }
      try {
        await s3.send(new ListObjectsV2Command({
          Bucket: bucket,
          Prefix: prefix || undefined,
          MaxKeys: 1,
        }));
        res.json({ success: true, message: `Connected to s3://${bucket}/${prefix || ''}` });
      } catch (s3Error) {
        res.json({ success: false, message: `Could not reach bucket: ${s3Error.message}` });
      }
    } catch (e) {
      console.error('POST /api/s3/test error:', e);
      jsonError(res, 500, 'InternalError', e.message);
    }
  });

  // ─── STUDIO: Workflows (Jobs & Workflow Builder) ────────────────────────
  //
  // Row shape — Workflows are a SHARED, independent entity, not owned by
  // one Application. Any number of Applications can be mapped to reuse the
  // same pipeline, and the mapping is editable after creation.
  //
  //   PK          SK            type       body
  //   "WORKFLOW"  "<wfId>"      WORKFLOW   { name, definition, status, stateMachineArn?, mappedApplicationIds }
  //
  // Flat partition (same pattern Dictionaries already use) — listing every
  // workflow is a plain Query on PK='WORKFLOW', no GSI needed.
  //
  // Deliberately ONE row per workflow, not a separate draft/production
  // pair — `definition` IS the draft (PUT never touches Step Functions).
  // `status` + `stateMachineArn` are metadata describing whether/where
  // it's been deployed; there's no separate frozen "production" copy of
  // the definition to keep in sync, since the real running definition
  // lives in Step Functions itself, not duplicated here.
  //
  // The canvas's data model IS Amazon States Language directly — no
  // translation layer. `definition` is passed to CreateStateMachine /
  // UpdateStateMachine exactly as stored.

  function toWorkflowSummary(row) {
    const workflowId = row.SK;
    const body       = row.body || {};
    const definition = body.definition || { StartAt: '', States: {} };
    return {
      workflowId,
      name:                 body.name || workflowId,
      status:                body.status || 'draft',
      stateCount:            Object.keys(definition.States || {}).length,
      stateMachineArn:       body.stateMachineArn,
      mappedApplicationIds:  body.mappedApplicationIds || [],
      version:               row.version || 1,
      lastModifiedAt:        row.lastModifiedAt,
      lastModifiedBy:        row.lastModifiedBy || STUDIO_USER,
    };
  }

  function toWorkflowRecord(row) {
    const summary = toWorkflowSummary(row);
    const body     = row.body || {};
    return { ...summary, definition: body.definition || { StartAt: '', States: {} } };
  }

  async function ddbConfigListAllWorkflows() {
    const items = [];
    let ExclusiveStartKey;
    do {
      const resp = await ddb.send(new QueryCommand({
        TableName: CONFIG_TABLE,
        KeyConditionExpression: 'PK = :pk',
        ExpressionAttributeValues: { ':pk': 'WORKFLOW' },
        ExclusiveStartKey,
      }));
      items.push(...(resp.Items || []));
      ExclusiveStartKey = resp.LastEvaluatedKey;
    } while (ExclusiveStartKey);
    return items;
  }

  /** Validates every id in `applicationIds` corresponds to a real Application.
   *  Returns the first invalid id, or null if all are valid. */
  async function firstInvalidApplicationId(applicationIds) {
    for (const appId of applicationIds || []) {
      const row = await ddbConfigGet(`APP#${appId}`, 'META');
      if (!row) return appId;
    }
    return null;
  }

  app.get('/api/workflows', async (_req, res) => {
    try {
      const rows = await ddbConfigListAllWorkflows();
      const workflows = rows.map(toWorkflowSummary);
      workflows.sort((a, b) => a.name.localeCompare(b.name));
      res.json({ workflows });
    } catch (e) {
      console.error('GET /api/workflows error:', e);
      jsonError(res, 500, 'InternalError', e.message);
    }
  });

  app.get('/api/workflows/:workflowId', async (req, res) => {
    try {
      const { workflowId } = req.params;
      const row = await ddbConfigGet('WORKFLOW', workflowId);
      if (!row) return jsonError(res, 404, 'NotFound', `Workflow ${workflowId} not found`);
      res.json(toWorkflowRecord(row));
    } catch (e) {
      console.error('GET /api/workflows/:workflowId error:', e);
      jsonError(res, 500, 'InternalError', e.message);
    }
  });

  app.post('/api/workflows', async (req, res) => {
    try {
      const { workflowId, name, applicationIds } = req.body || {};
      if (!workflowId || !/^[A-Z0-9_]+$/.test(String(workflowId))) {
        return jsonError(res, 400, 'BadRequest',
          'workflowId is required and must be uppercase alphanumerics/underscores.');
      }

      const existing = await ddbConfigGet('WORKFLOW', workflowId);
      if (existing) {
        return jsonError(res, 409, 'AlreadyExists', `Workflow ${workflowId} already exists`);
      }

      const invalidAppId = await firstInvalidApplicationId(applicationIds);
      if (invalidAppId) {
        return jsonError(res, 400, 'BadRequest', `Application ${invalidAppId} does not exist`);
      }

      // Minimal starter — a single Task state, nothing wired to a real
      // function yet. The person fills in the Resource ARN via the
      // inspector once they land on the canvas.
      const starterStateName = 'FirstState';
      const definition = {
        Comment: name || workflowId,
        StartAt: starterStateName,
        States: {
          [starterStateName]: { Type: 'Task', Resource: '', End: true },
        },
      };

      const item = {
        PK:   'WORKFLOW',
        SK:   workflowId,
        type: 'WORKFLOW',
        body: {
          name: name || workflowId,
          definition,
          status: 'draft',
          mappedApplicationIds: applicationIds || [],
        },
      };
      const written = await ddbConfigPutRow(item, null);
      res.status(201).json(toWorkflowRecord(written));
    } catch (e) {
      console.error('POST /api/workflows error:', e);
      jsonError(res, 500, 'InternalError', e.message);
    }
  });

  // Save draft and/or update the Application mapping — writes to DynamoDB
  // ONLY. Never calls Step Functions. Accepts either or both fields so the
  // canvas's Save (definition) and the "Manage Applications" modal
  // (mappedApplicationIds) can share one endpoint without stepping on
  // each other's concerns.
  app.put('/api/workflows/:workflowId', async (req, res) => {
    try {
      const { workflowId } = req.params;
      const expected = requireExpectedVersion(req, res);
      if (expected == null) return;
      const existing = await ddbConfigGet('WORKFLOW', workflowId);
      if (!existing) return jsonError(res, 404, 'NotFound', `Workflow ${workflowId} not found`);

      const { definition, mappedApplicationIds } = req.body || {};
      if (definition === undefined && mappedApplicationIds === undefined) {
        return jsonError(res, 400, 'BadRequest',
          'Provide at least one of: definition, mappedApplicationIds.');
      }
      if (definition !== undefined && (!definition.StartAt || !definition.States)) {
        return jsonError(res, 400, 'BadRequest', 'definition must include StartAt and States.');
      }
      if (mappedApplicationIds !== undefined) {
        const invalidAppId = await firstInvalidApplicationId(mappedApplicationIds);
        if (invalidAppId) {
          return jsonError(res, 400, 'BadRequest', `Application ${invalidAppId} does not exist`);
        }
      }

      const body = existing.body || {};
      const item = {
        PK:   'WORKFLOW',
        SK:   workflowId,
        type: 'WORKFLOW',
        body: {
          ...body,
          ...(definition !== undefined ? { definition } : {}),
          ...(mappedApplicationIds !== undefined ? { mappedApplicationIds } : {}),
        },
      };
      const written = await ddbConfigPutRow(item, expected);
      res.json(toWorkflowRecord(written));
    } catch (e) {
      if (e.code === 'VersionConflict') {
        return respondVersionConflict(res, 'WORKFLOW', req.params.workflowId);
      }
      console.error('PUT /api/workflows/:workflowId error:', e);
      jsonError(res, 500, 'InternalError', e.message);
    }
  });

  app.delete('/api/workflows/:workflowId', async (req, res) => {
    try {
      await ddbConfigDelete('WORKFLOW', req.params.workflowId);
      res.status(204).end();
    } catch (e) {
      console.error('DELETE /api/workflows/:workflowId error:', e);
      jsonError(res, 500, 'InternalError', e.message);
    }
  });

  // Deploy = create-or-update the real Step Functions state machine from
  // the current draft. The ONLY route in this whole file that calls the
  // Step Functions API. Never creates a new IAM role — WORKFLOW_ROLE_ARN
  // must already exist and already trust states.amazonaws.com.
  app.post('/api/workflows/:workflowId/deploy', async (req, res) => {
    try {
      const { workflowId } = req.params;
      const expected = requireExpectedVersion(req, res);
      if (expected == null) return;

      const existing = await ddbConfigGet('WORKFLOW', workflowId);
      if (!existing) return jsonError(res, 404, 'NotFound', `Workflow ${workflowId} not found`);
      if (existing.version !== expected) {
        return respondVersionConflict(res, 'WORKFLOW', workflowId);
      }

      const body = existing.body || {};
      const definition = body.definition;
      if (!definition) return jsonError(res, 400, 'BadRequest', 'No definition to deploy.');

      const definitionJson = JSON.stringify(definition);
      let action, stateMachineArn;

      if (body.stateMachineArn) {
        await sfn.send(new UpdateStateMachineCommand({
          stateMachineArn: body.stateMachineArn,
          definition: definitionJson,
        }));
        action = 'updated';
        stateMachineArn = body.stateMachineArn;
      } else {
        // No more app prefix in the generated name — a workflow isn't
        // owned by one application anymore.
        const stateMachineName = `idp-${workflowId}`.toLowerCase().slice(0, 80);
        const createResp = await sfn.send(new CreateStateMachineCommand({
          name: stateMachineName,
          definition: definitionJson,
          roleArn: WORKFLOW_ROLE_ARN,
          type: 'STANDARD',
        }));
        action = 'created';
        stateMachineArn = createResp.stateMachineArn;
      }

      const item = {
        PK:   'WORKFLOW',
        SK:   workflowId,
        type: 'WORKFLOW',
        body: { ...body, status: 'deployed', stateMachineArn },
      };
      const written = await ddbConfigPutRow(item, expected);

      res.json({ stateMachineArn, action, version: written.version });
    } catch (e) {
      if (e.code === 'VersionConflict') {
        return respondVersionConflict(res, 'WORKFLOW', req.params.workflowId);
      }
      console.error('POST /api/workflows/:workflowId/deploy error:', e);
      jsonError(res, 500, 'InternalError',
        `Deploy failed: ${e.message}. If this is an IAM error, verify WORKFLOW_ROLE_ARN ` +
        `(${WORKFLOW_ROLE_ARN}) trusts states.amazonaws.com and iam:PassRole isn't blocked ` +
        `by the account boundary policy.`);
    }
  });


  // -----------------------------------------------------------------------
  // 404 fallback
  // -----------------------------------------------------------------------
  app.use((req, res) => {
    jsonError(res, 404, 'NotFound', `No route for ${req.method} ${req.originalUrl}`);
  });
   
  // -----------------------------------------------------------------------
  // Boot
  // -----------------------------------------------------------------------
  // =======================================================================
  // LAMBDA ENTRYPOINT
  // =======================================================================
  let _serverlessHandler = null;
  function getServerlessHandler() {
    if (!_serverlessHandler) {
      const serverlessExpress = require('@vendia/serverless-express');
      _serverlessHandler = serverlessExpress({ app });
    }
    return _serverlessHandler;
  }
  exports.handler = async (event, context) => {
    return getServerlessHandler()(event, context);
  };
  // =======================================================================
  // LOCAL DEV SERVER (only starts when run directly: node index.js)
  // =======================================================================
  if (require.main === module) {
    app.listen(PORT, () => {
      console.log('==============================================');
      console.log(' IDP - Local API Proxy');
      console.log('==============================================');
      console.log(` Listening:   http://localhost:${PORT}`);
      console.log(` Region:      ${REGION}`);
      console.log(` Table:       ${TABLE}`);
      console.log(` Docs bucket: ${DOCS_BUCKET}`);
      console.log(` Config key:  ${CONFIG_KEY}`);
      console.log('');
      console.log(' Quick test:');
      console.log(`   curl http://localhost:${PORT}/api/health`);
      console.log(`   curl http://localhost:${PORT}/api/applications`);
      console.log(`   curl 'http://localhost:${PORT}/api/batches?application=AFLCOR'`);
      console.log('==============================================');
    });
  }