import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoDBStreamsClient } from '@aws-sdk/client-dynamodb-streams'
import { commonConfig } from './aws-config.js'

// DynamoDBDocumentClient is deliberately excluded.
// Raw AttributeValue maps test DynamoDB's type system more precisely.

/** Low-level DynamoDB client */
export const ddb = new DynamoDBClient(commonConfig)

/** DynamoDB Streams client */
export const ddbStreams = new DynamoDBStreamsClient(commonConfig)
