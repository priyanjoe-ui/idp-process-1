#!/usr/bin/env node
/**
 * migrate-workflow-to-ddb.js
 * ------------------------------------------------------------------
 * One-shot import of an EXISTING, already-deployed Step Functions state
 * machine into the idp-config DynamoDB table, so Studio's Jobs & Workflow
 * Builder can show and edit it.
 *
 * This is the Workflows equivalent of migrate-config-to-ddb.js — same
 * shape, same CONFIG-block-then-run pattern, same --dry-run/--force flags.
 * Unlike that script (which reads a JSON file from S3), this one calls
 * DescribeStateMachine on the real ARN, so it pulls whatever is ACTUALLY
 * deployed right now — not a stale copy of the definition from a doc or
 * an earlier design conversation.
 *
 * Row written:
 *   PK              SK                    type       body
 *   "APP#<appId>"   "WORKFLOW#<wfId>"     WORKFLOW   { name, definition, status: 'deployed', stateMachineArn }
 *
 * Usage
 * -----
 *   1. Fill in the CONFIG block below (state machine ARN, target app/workflow IDs).
 *   2. From the idp-api-proxy directory in CloudShell:
 *        npm install                    # once, if you haven't
 *        node scripts/migrate-workflow-to-ddb.js --dry-run
 *        node scripts/migrate-workflow-to-ddb.js
 *
 * Behavior
 * --------
 *   - Idempotent: refuses to overwrite an existing row unless you pass --force.
 *   - Dry run: pass --dry-run to see what would be written without touching DDB.
 *   - Does NOT touch the real state machine — read-only against Step Functions
 *     (DescribeStateMachine only). Nothing here calls CreateStateMachine or
 *     UpdateStateMachine.
 *   - Run again later with a different STATE_MACHINE_ARN/APP_ID/WORKFLOW_ID
 *     to bring in additional existing pipelines as you onboard more
 *     applications — same script, different CONFIG values each time.
 * ------------------------------------------------------------------
 */

// ═══════════════════════════════════════════════════════════════════
// CONFIG — FILL THESE IN
// ═══════════════════════════════════════════════════════════════════
const CONFIG = {
  // The existing, already-running state machine to import.
  STATE_MACHINE_ARN: '<YOUR_STATE_MACHINE_ARN>',  // e.g. 'arn:aws:states:us-east-2:651720177345:stateMachine:idp-ingestion'

  // Where it lands in Studio.
  APP_ID:      'AFLCOR',           // must already exist as an Application in Studio
  WORKFLOW_ID: 'MAIN_PIPELINE',    // uppercase alphanumerics/underscores, becomes the Studio workflow ID
  NAME:        'Main IDP Pipeline',// display name shown in Studio's workflow list

  // Target: DDB table (must already exist with GSI 'type-lastModifiedAt-index')
  TABLE:      'idp-config',
  AWS_REGION: 'us-east-2',
};
// ═══════════════════════════════════════════════════════════════════

const { SFNClient, DescribeStateMachineCommand } = require('@aws-sdk/client-sfn');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const {
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
} = require('@aws-sdk/lib-dynamodb');

const args = new Set(process.argv.slice(2));
const DRY_RUN = args.has('--dry-run');
const FORCE   = args.has('--force');

const sfn = new SFNClient({ region: CONFIG.AWS_REGION });
const ddb = DynamoDBDocumentClient.from(
  new DynamoDBClient({ region: CONFIG.AWS_REGION }),
  { marshallOptions: { removeUndefinedValues: true } },
);

const NOW = new Date().toISOString();
const MIGRATION_USER = 'migration';

function log(...a)  { console.log(...a); }
function die(msg)   { console.error('ERROR:', msg); process.exit(1); }

async function rowExists(pk, sk) {
  const resp = await ddb.send(new GetCommand({
    TableName: CONFIG.TABLE,
    Key: { PK: pk, SK: sk },
    ProjectionExpression: 'PK',
  }));
  return !!resp.Item;
}

async function main() {
  if (CONFIG.STATE_MACHINE_ARN.startsWith('<')) {
    die('Fill in CONFIG.STATE_MACHINE_ARN at the top of this file.');
  }
  if (!/^[A-Z0-9_]+$/.test(CONFIG.WORKFLOW_ID)) {
    die('CONFIG.WORKFLOW_ID must be uppercase letters, numbers, and underscores only.');
  }

  log('══════════════════════════════════════════════════');
  log(' migrate-workflow-to-ddb.js');
  log(`   source :  ${CONFIG.STATE_MACHINE_ARN}`);
  log(`   target :  APP#${CONFIG.APP_ID}  SK=WORKFLOW#${CONFIG.WORKFLOW_ID}`);
  log(`   table  :  ${CONFIG.TABLE}  (${CONFIG.AWS_REGION})`);
  log(`   mode   :  ${DRY_RUN ? 'DRY RUN' : FORCE ? 'FORCE OVERWRITE' : 'skip-if-exists'}`);
  log('══════════════════════════════════════════════════');

  // Read-only call — describes the state machine, never modifies it.
  log(`\nFetching current definition from Step Functions...`);
  const described = await sfn.send(new DescribeStateMachineCommand({
    stateMachineArn: CONFIG.STATE_MACHINE_ARN,
  }));

  let definition;
  try {
    definition = JSON.parse(described.definition);
  } catch (e) {
    die(`Could not parse the state machine's definition as JSON: ${e.message}`);
  }

  const stateCount = Object.keys(definition.States || {}).length;
  log(`  found ${stateCount} states, StartAt: "${definition.StartAt}"`);
  log(`  role  : ${described.roleArn}`);
  log(`  status: ${described.status}`);

  const pk = `APP#${CONFIG.APP_ID}`;
  const sk = `WORKFLOW#${CONFIG.WORKFLOW_ID}`;

  if (DRY_RUN) {
    log(`\n[dry-run] would write PK=${pk}  SK=${sk}`);
    log('[dry-run] body.definition:');
    log(JSON.stringify(definition, null, 2));
    log('\n══════════════════════════════════════════════════');
    log(' Done (DRY RUN — no writes).');
    log('══════════════════════════════════════════════════');
    return;
  }

  if (!FORCE && await rowExists(pk, sk)) {
    log(`\nSkipping — a workflow already exists at PK=${pk} SK=${sk} (use --force to overwrite).`);
    return;
  }

  const item = {
    PK: pk,
    SK: sk,
    type: 'WORKFLOW',
    body: {
      name: CONFIG.NAME,
      definition,
      status: 'deployed',
      stateMachineArn: CONFIG.STATE_MACHINE_ARN,
    },
    version: 1,
    lastModifiedAt: NOW,
    lastModifiedBy: MIGRATION_USER,
  };

  await ddb.send(new PutCommand({ TableName: CONFIG.TABLE, Item: item }));
  log(`\nWrote PK=${pk}  SK=${sk}`);

  log('\n══════════════════════════════════════════════════');
  log(' Done. Open Studio → Applications → ' + CONFIG.APP_ID + ' → Jobs & Workflows to view it.');
  log('══════════════════════════════════════════════════');
}

main().catch(err => {
  console.error('\nMIGRATION FAILED:');
  console.error(err);
  process.exit(1);
});
