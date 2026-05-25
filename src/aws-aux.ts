import { S3Client } from '@aws-sdk/client-s3'
import { KinesisClient } from '@aws-sdk/client-kinesis'
import { ApplicationAutoScalingClient } from '@aws-sdk/client-application-auto-scaling'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { commonConfig, credentials, endpoint } from './aws-config.js'

// Auxiliary service clients for the control-plane operations that depend on
// other AWS services. They mirror the DynamoDB client's endpoint logic: against
// a local emulator they target DYNAMODB_ENDPOINT (LocalStack co-locates these
// services), against real AWS they use the resolved region. An emulator that
// does not expose a given service fails the control-plane support probe and the
// dependent test skips.

/**
 * S3 client (table export / import). Path-style addressing is forced for
 * emulator endpoints, where virtual-hosted-style bucket hostnames do not
 * resolve.
 */
export const s3 = new S3Client(
  endpoint ? { ...commonConfig, forcePathStyle: true } : commonConfig,
)

/** Kinesis client (streaming destination). */
export const kinesis = new KinesisClient(commonConfig)

/** Application Auto Scaling client (replica autoscaling). */
export const appAutoScaling = new ApplicationAutoScalingClient(commonConfig)

/**
 * Second region used for global-table / multi-region coverage. Overridable so
 * the suite is not pinned to one primary region.
 */
export const replicaRegion =
  process.env.CONFORMANCE_REPLICA_REGION || 'eu-west-1'

/**
 * A DynamoDB client pinned to a specific region. On real AWS this targets that
 * region directly; against an emulator it keeps the single configured endpoint,
 * since emulators expose one region.
 */
export function ddbInRegion(regionOverride: string): DynamoDBClient {
  return new DynamoDBClient(
    endpoint
      ? { endpoint, region: regionOverride, credentials }
      : { region: regionOverride },
  )
}
