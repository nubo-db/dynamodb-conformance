/** DynamoDB key attribute types */
export type DynamoDBKeyType = 'S' | 'N' | 'B'

/** A key attribute definition (name + type) */
export interface KeyDef {
  name: string
  type: DynamoDBKeyType
}

/** GSI/LSI projection type */
export type ProjectionType = 'ALL' | 'KEYS_ONLY' | 'INCLUDE'

/** Table definition used by the helper to create/teardown test tables */
export interface TestTableDef {
  name: string
  hashKey: KeyDef
  rangeKey?: KeyDef
  billingMode?: 'PROVISIONED' | 'PAY_PER_REQUEST'
  gsis?: {
    indexName: string
    hashKey: KeyDef
    rangeKey?: KeyDef
    projectionType: ProjectionType
    nonKeyAttributes?: string[]
  }[]
  lsis?: {
    indexName: string
    rangeKey: KeyDef
    projectionType: ProjectionType
    nonKeyAttributes?: string[]
  }[]
}
