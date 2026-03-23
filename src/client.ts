import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoDBStreamsClient } from '@aws-sdk/client-dynamodb-streams'

// DynamoDBDocumentClient is deliberately excluded.
// Raw AttributeValue maps test DynamoDB's type system more precisely.

const endpoint = process.env.DYNAMODB_ENDPOINT
const region = process.env.AWS_REGION || 'us-east-1'

const commonConfig = {
  ...(endpoint ? { endpoint } : {}),
  region,
  // For local endpoints, any credentials work
  ...(endpoint
    ? {
        credentials: {
          accessKeyId: process.env.AWS_ACCESS_KEY_ID || 'fakeAccessKeyId',
          secretAccessKey:
            process.env.AWS_SECRET_ACCESS_KEY || 'fakeSecretAccessKey',
        },
      }
    : {}),
}

/** Low-level DynamoDB client */
export const ddb = new DynamoDBClient(commonConfig)

/** DynamoDB Streams client */
export const ddbStreams = new DynamoDBStreamsClient(commonConfig)
