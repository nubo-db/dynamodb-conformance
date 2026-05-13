import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoDBStreamsClient } from '@aws-sdk/client-dynamodb-streams'

// DynamoDBDocumentClient is deliberately excluded.
// Raw AttributeValue maps test DynamoDB's type system more precisely.

const endpoint = process.env.DYNAMODB_ENDPOINT
const region = process.env.AWS_REGION || 'us-east-1'

// Used only when DYNAMODB_ENDPOINT is set (local emulator targets).
const accessKeyId = process.env.AWS_ACCESS_KEY_ID || 'fakeAccessKeyId'
const secretAccessKey =
  process.env.AWS_SECRET_ACCESS_KEY || 'fakeSecretAccessKey'
const sessionToken = process.env.AWS_SESSION_TOKEN

// Local endpoints accept any credentials. AWS_SESSION_TOKEN is forwarded
// when set so emulators that authenticate via the SigV4 session-token
// header can be exercised.
const credentials = sessionToken
  ? { accessKeyId, secretAccessKey, sessionToken }
  : { accessKeyId, secretAccessKey }

const commonConfig = endpoint
  ? { endpoint, region, credentials }
  : { region }

/** Low-level DynamoDB client */
export const ddb = new DynamoDBClient(commonConfig)

/** DynamoDB Streams client */
export const ddbStreams = new DynamoDBStreamsClient(commonConfig)
