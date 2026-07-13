# Test Bench — Deployment Runbook

Deploys the backend for Studio's Test Bench feature: synchronous classification + extraction against an uploaded sample, without touching the real Step Functions pipeline or any batch-tracking data.

**Scope of this runbook:** backend only — the shared layer, two refactored Lambdas, one new Lambda, and the proxy route. **The Studio UI (dropdowns, drop zone, results view) is a separate piece of work not covered here.**

**Region/account used in examples:** `us-east-2` / `651720177345` — replace with your own.

---

## What's changing, in one picture

```
                    ┌─────────────────────────┐
                    │   idp-shared layer (v3) │
                    │                         │
                    │  config_loader.py       │  (existing, DynamoDB-based)
                    │  ddb.py                 │  (existing, unchanged)
                    │  classification_logic.py│  ← NEW
                    │  extraction_logic.py    │  ← NEW
                    └───────────┬─────────────┘
                    ┌───────────┼───────────────────┐
                    │           │                   │
          ┌─────────▼──────────┐  ┌▼───────────────────┐
          │ idp-classification │  │idp-extraction-callback│
          │  (refactored to    │  │  (refactored to      │
          │   import shared    │  │   import shared       │
          │   logic, same      │  │   logic, same          │
          │   behavior)        │  │   behavior)             │
          └────────────────────┘  └─────────────────────────┘

                                       ┌─────────────────┐
          idp-api-proxy  ──invoke()──▶ │  idp-test-bench │  ← NEW
          (new route added)            │  (new function) │
                                       └─────────────────┘
```

One shared layer, two existing Lambdas refactored to pull their core logic from it (zero behavior change), one brand-new Lambda that reuses the same logic synchronously, and one new proxy route wiring it all together.

---

## Files you should have from this conversation

| File | What it is |
|---|---|
| `idp-shared-layer-v3.zip` | Complete shared layer contents — all 5 files, not just the 2 new ones |
| `idp-classification-updated.zip` | Refactored `idp-classification` — imports `classification_logic` instead of defining it locally |
| `idp-extraction-callback-updated.zip` | Refactored `idp-extraction-callback` — imports `extraction_logic`; also carries the earlier `MaxWordCount` blank-page fix |
| `idp-test-bench-new.zip` | The new Lambda |
| `pillow-layer.zip` | Real `manylinux2014_x86_64` Pillow wheel, built for Lambda's Python 3.12 runtime |
| `index.js` | Updated proxy — new `POST /api/test-bench/run` route |

If any of these aren't at hand, they were generated earlier in this conversation — regenerate from the corresponding `.py` source files rather than hand-editing a zip.

---

## Step 1 — Publish the updated shared layer

Publishing a layer version **replaces the entire contents** — this zip must contain everything the layer currently needs, not just the 2 new files, or you'll silently lose `config_loader.py`'s DynamoDB support and `ddb.py` for every function using this layer.

```bash
aws lambda publish-layer-version \
  --layer-name idp-shared \
  --description "Adds classification_logic.py + extraction_logic.py for Test Bench" \
  --zip-file fileb://idp-shared-layer-v3.zip \
  --compatible-runtimes python3.12
```

Note the returned `LayerVersionArn` (e.g. `arn:aws:lambda:us-east-2:651720177345:layer:idp-shared:6`) — you'll need the version number in the next step.

---

## Step 2 — Deploy the two refactored Lambdas

```bash
# idp-classification
aws lambda update-function-code \
  --function-name idp-classification \
  --zip-file fileb://idp-classification-updated.zip

aws lambda update-function-configuration \
  --function-name idp-classification \
  --layers arn:aws:lambda:us-east-2:651720177345:layer:idp-shared:6

# idp-extraction-callback
aws lambda update-function-code \
  --function-name idp-extraction-callback \
  --zip-file fileb://idp-extraction-callback-updated.zip

aws lambda update-function-configuration \
  --function-name idp-extraction-callback \
  --layers arn:aws:lambda:us-east-2:651720177345:layer:idp-shared:6
```

**Caveat:** `--layers` replaces the *entire* list for that function. If either function has other layers attached, list all of them here, not just this one.

**Verify no regression:** run one real batch through the pipeline after this step, before moving on. Check CloudWatch Logs for both functions — behavior should be identical to before the refactor, since the logic itself didn't change, only its location.

---

## Step 3 — Publish the Pillow layer

```bash
aws lambda publish-layer-version \
  --layer-name pillow \
  --zip-file fileb://pillow-layer.zip \
  --compatible-runtimes python3.12 \
  --compatible-architectures x86_64
```

This is a genuine `manylinux2014_x86_64` wheel — not a plain local `pip install`, which would ship the wrong binary and fail silently at runtime with an import error.

Note the returned ARN (e.g. `arn:aws:lambda:us-east-2:651720177345:layer:pillow:1`).

---

## Step 4 — Create the `idp-test-bench` IAM role

Create an execution role (if one doesn't already exist for this purpose) with:

- **Trust policy:** standard Lambda trust relationship (`lambda.amazonaws.com`)
- **Permissions:**
  - `AWSLambdaBasicExecutionRole` (managed policy — CloudWatch Logs)
  - `textract:AnalyzeDocument`, `textract:DetectDocumentText`
  - `dynamodb:Query` on `idp-config` and `idp-config/index/type-lastModifiedAt-index` (same as `idp-classification` already has)

Example inline policy for the Textract + DynamoDB permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["textract:AnalyzeDocument", "textract:DetectDocumentText"],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": "dynamodb:Query",
      "Resource": [
        "arn:aws:dynamodb:us-east-2:651720177345:table/idp-config",
        "arn:aws:dynamodb:us-east-2:651720177345:table/idp-config/index/type-lastModifiedAt-index"
      ]
    }
  ]
}
```

No S3 permissions needed — the uploaded file never gets written to S3.

---

## Step 5 — Create the `idp-test-bench` Lambda

```bash
aws lambda create-function \
  --function-name idp-test-bench \
  --runtime python3.12 \
  --handler handler.lambda_handler \
  --zip-file fileb://idp-test-bench-new.zip \
  --role arn:aws:iam::651720177345:role/<TEST_BENCH_EXECUTION_ROLE> \
  --timeout 120 \
  --memory-size 512 \
  --layers arn:aws:lambda:us-east-2:651720177345:layer:idp-shared:6 \
           arn:aws:lambda:us-east-2:651720177345:layer:pillow:1
```

**Timeout is 120s, not the default 3s** — a 5-page document means 5 sequential synchronous Textract calls, each a couple of seconds. Don't skip raising this.

**No Function URL needed.** The proxy reaches this function via `lambda:invoke` (a direct SDK call using the function's ARN/name), not an HTTP endpoint — simpler and keeps this function off the public internet entirely.

---

## Step 6 — Deploy the updated proxy

```bash
# In the idp-api-proxy project, with index.js already replaced:
npm run build:lambda
npm run package
aws lambda update-function-code \
  --function-name idp-api-proxy \
  --zip-file fileb://lambda-deployment.zip
```

No new npm dependencies — `@aws-sdk/client-lambda`'s `InvokeCommand` was already available, just not previously imported.

If you named the new function something other than `idp-test-bench`, set:
```bash
aws lambda update-function-configuration \
  --function-name idp-api-proxy \
  --environment "Variables={<existing vars...>,IDP_TEST_BENCH_FUNCTION_NAME=<your-name>}"
```
(`--environment` replaces the whole variable set — merge with what's already there, don't overwrite it.)

---

## Step 7 — Grant the proxy permission to invoke the new function

Easy to forget, and the failure mode is a confusing `AccessDenied` at request time, not at deploy time:

```bash
aws iam put-role-policy \
  --role-name <IDP_API_PROXY_EXECUTION_ROLE> \
  --policy-name InvokeTestBench \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Action": "lambda:InvokeFunction",
      "Resource": "arn:aws:lambda:us-east-2:651720177345:function:idp-test-bench"
    }]
  }'
```

---

## Step 8 — Verify, in this order

**8a. Test `idp-test-bench` directly, in isolation** — before routing through the proxy, so any failure here isn't confused with a proxy/IAM problem:

```bash
# Build a test payload
python3 -c "
import base64, json
with open('sample.tif', 'rb') as f:
    b64 = base64.b64encode(f.read()).decode()
payload = {
    'applicationId': 'AFLCOR',
    'documentTypeId': 'CORRDOC',
    'fileName': 'sample.tif',
    'fileBase64': b64,
}
with open('test-payload.json', 'w') as f:
    json.dump(payload, f)
"

aws lambda invoke \
  --function-name idp-test-bench \
  --payload "$(base64 -i test-payload.json)" \
  --cli-binary-format raw-in-base64-out \
  response.json

cat response.json
```

Expect a JSON response with `pageCount`, and a `pages` array with `pageType` and `fields` per page.

**8b. Test through the proxy:**

```bash
curl -X POST https://<your-function-url>/api/test-bench/run \
  -H "Content-Type: application/json" \
  -d @test-payload.json
```

If 8a passes but 8b fails, the problem is isolated to the proxy/IAM wiring (Step 7), not the classification/extraction logic itself.

**8c. Confirm the real pipeline still works** — run one real batch through end to end (S3 drop → ingestion → classification → extraction → validation), and check CloudWatch Logs for `idp-classification` and `idp-extraction-callback` to confirm identical behavior to before this deployment. This is the regression check for Step 2's refactor.

**8d. Try a real multi-page TIFF and a real ZIP sample** through 8a/8b — the smoke tests already run confirm the splitting logic works on synthetic images; a real scanned document is the first genuine end-to-end validation.

---

## Known constraints, going in

- **6MB Lambda Function URL payload ceiling** (hard limit, not configurable). Base64 inflates a file's size by ~33%, capping a realistic test sample at roughly 4MB raw. Fine for typical scans; try your actual largest realistic sample before considering this done. If it's a real problem, the fix is routing large files through a scratch S3 upload instead of this direct-invoke path — don't build that pre-emptively unless you hit it.
- **No PDF support in v1** — only `.tif`/`.tiff` and `.zip` (containing `.tif`/`.tiff`), matching what real ingestion already handles. Adding PDF later means a new `pdf2image` + poppler dependency.
- **Not instant** — each page is a real synchronous Textract call (~1–3 seconds). A 5-page document could take 10–15 seconds end to end. The Studio UI (not yet built) should have a visible loading state sized for that, not assume sub-second response.
- **This is backend-only.** Studio's actual Test Bench page — Application/Document Type dropdowns, the drop zone, the per-page results view — still needs to be built against this now-working `POST /api/test-bench/run` endpoint.
