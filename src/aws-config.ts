// Shared AWS client configuration.
//
// DYNAMODB_ENDPOINT selects a local emulator target; when it is unset the
// clients talk to real AWS by region. The auxiliary service clients (S3,
// Kinesis, Application Auto Scaling — see aws-aux.ts) reuse this same config so
// a LocalStack run reaches its co-located services at the same endpoint, while
// an emulator without those services simply fails the control-plane support
// probe (see infra.ts).

const endpoint = process.env.DYNAMODB_ENDPOINT
const region = process.env.AWS_REGION || 'us-east-1'

// Used only when DYNAMODB_ENDPOINT is set (local emulator targets).
const accessKeyId = process.env.AWS_ACCESS_KEY_ID || 'fakeAccessKeyId'
const secretAccessKey =
  process.env.AWS_SECRET_ACCESS_KEY || 'fakeSecretAccessKey'
const sessionToken = process.env.AWS_SESSION_TOKEN

// Local endpoints accept any credentials. AWS_SESSION_TOKEN is forwarded when
// set so emulators that authenticate via the SigV4 session-token header can be
// exercised.
const credentials = sessionToken
  ? { accessKeyId, secretAccessKey, sessionToken }
  : { accessKeyId, secretAccessKey }

/** True when pointed at a local emulator (DYNAMODB_ENDPOINT is set). */
export const isEmulator = Boolean(endpoint)

export { endpoint, region, credentials }

/** Base config shared by every AWS client in the suite. */
export const commonConfig = endpoint
  ? { endpoint, region, credentials }
  : { region }
