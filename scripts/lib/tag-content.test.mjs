import { describe, it, expect } from 'vitest'
import { TAG_NAMES } from '../../src/tags.js'
import {
  CAPABILITY_MARKERS,
  capabilityLeaks,
  parseBlocks,
  resolveTags,
  stripLiterals,
} from './tag-content.mjs'

const tags = (leaks) => leaks.map((l) => `${l.scope}:${l.title}:${l.tag}`)

describe('stripLiterals', () => {
  it('blanks string contents but keeps the quotes and the line count', () => {
    const out = stripLiterals("const a = 'hello'\nconst b = 1\n")
    expect(out).toBe("const a = '     '\nconst b = 1\n")
  })

  it('blanks braces inside strings so they cannot move the brace depth', () => {
    const out = stripLiterals("expect(m).toContain('parameters: {Expected}')")
    expect(out).not.toContain('{')
    expect(out).not.toContain('}')
  })

  it('blanks line and block comments', () => {
    expect(stripLiterals('const a = 1 // Expected: S\n')).not.toContain('Expected')
    expect(stripLiterals('/* AttributesToGet: x */\n')).not.toContain('AttributesToGet')
  })

  it('does not treat an apostrophe inside a double-quoted string as a quote', () => {
    const out = stripLiterals('const a = "don\'t"\nconst b = 2\n')
    expect(out.split('\n')[1]).toBe('const b = 2')
  })
})

describe('parseBlocks', () => {
  it('records a describe, its tests, and the tags on each', () => {
    const src = [
      "describe('Outer', { tags: ['get-item', 'data-plane'] }, () => {",
      "  it('plain', async () => {",
      '    expect(1).toBe(1)',
      '  })',
      "  it('tagged', { tags: ['legacy'] }, async () => {",
      '    expect(1).toBe(1)',
      '  })',
      '})',
      '',
    ].join('\n')
    const [outer] = parseBlocks(src)
    expect(outer.kind).toBe('describe')
    expect(outer.title).toBe('Outer')
    expect(outer.tags).toEqual(['get-item', 'data-plane'])
    expect(outer.children.map((c) => [c.title, c.tags])).toEqual([
      ['plain', []],
      ['tagged', ['legacy']],
    ])
  })

  it('nests a describe inside a describe', () => {
    const src = [
      "describe('Outer', { tags: ['scan'] }, () => {",
      "  describe('Inner', { tags: ['legacy'] }, () => {",
      "    it('deep', async () => {",
      '      expect(1).toBe(1)',
      '    })',
      '  })',
      '})',
      '',
    ].join('\n')
    const [outer] = parseBlocks(src)
    const inner = outer.children[0]
    expect(inner.title).toBe('Inner')
    expect(resolveTags([outer, inner, inner.children[0]])).toEqual(new Set(['scan', 'legacy']))
  })

  it('is not confused by a brace inside an asserted message', () => {
    const src = [
      "describe('Outer', { tags: ['put-item'] }, () => {",
      "  it('one', async () => {",
      "    expect(m).toContain('Non-expression parameters: {Expected}')",
      '  })',
      "  it('two', async () => {",
      '    expect(1).toBe(1)',
      '  })',
      '})',
      '',
    ].join('\n')
    const [outer] = parseBlocks(src)
    expect(outer.children.map((c) => c.title)).toEqual(['one', 'two'])
  })
})

describe('capabilityLeaks', () => {
  it('reports a legacy parameter on an untagged test', () => {
    const src = [
      "describe('PutItem', { tags: ['put-item', 'data-plane'] }, () => {",
      "  it('mixes them', async () => {",
      '    await ddb.send(new PutItemCommand({',
      '      TableName: t,',
      '      Expected: { pk: { Exists: false } },',
      '    }))',
      '  })',
      '})',
      '',
    ].join('\n')
    expect(tags(capabilityLeaks(src))).toEqual(['test:mixes them:legacy'])
  })

  it('reports a SearchVectors send on an untagged test', () => {
    const src = [
      "describe('Vector', { tags: ['query', 'data-plane'] }, () => {",
      "  it('searches', async () => {",
      '    await ddb.send(new SearchVectorsCommand({ TableName: t }))',
      '  })',
      '})',
      '',
    ].join('\n')
    expect(tags(capabilityLeaks(src))).toEqual(['test:searches:search-vectors'])
  })

  it('reports a vector-index table build on an untagged describe', () => {
    const src = [
      "describe('CreateTable', { tags: ['create-table', 'control-plane'] }, () => {",
      '  beforeAll(async () => {',
      '    await ddb.send(new CreateTableCommand({',
      '      VectorIndexes: [vix()],',
      '    }))',
      '  })',
      "  it('unrelated', async () => {",
      '    expect(1).toBe(1)',
      '  })',
      '})',
      '',
    ].join('\n')
    expect(tags(capabilityLeaks(src))).toEqual(['describe:CreateTable:vector'])
  })

  it('ignores marker names on a single-line import', () => {
    const src = [
      "import { CreateTableCommand, SearchVectorsCommand } from '@aws-sdk/client-dynamodb'",
      "describe('Vector', { tags: ['search-vectors', 'vector', 'data-plane'] }, () => {",
      "  it('searches', async () => {",
      '    await ddb.send(new SearchVectorsCommand({ TableName: t }))',
      '  })',
      '})',
      '',
    ].join('\n')
    expect(capabilityLeaks(src)).toEqual([])
  })

  it('is clean when the tag is on the test', () => {
    const src = [
      "describe('PutItem', { tags: ['put-item', 'data-plane'] }, () => {",
      "  it('mixes them', { tags: ['legacy'] }, async () => {",
      '      Expected: { pk: { Exists: false } },',
      '  })',
      '})',
      '',
    ].join('\n')
    expect(capabilityLeaks(src)).toEqual([])
  })

  it('is clean when the tag is inherited from the describe', () => {
    const src = [
      "describe('Legacy API', { tags: ['put-item', 'legacy', 'data-plane'] }, () => {",
      "  it('uses Expected', async () => {",
      '      Expected: { pk: { Exists: false } },',
      '  })',
      '})',
      '',
    ].join('\n')
    expect(capabilityLeaks(src)).toEqual([])
  })

  it('escalates a marker in shared setup to the describe', () => {
    const src = [
      "describe('PutItem', { tags: ['put-item', 'data-plane'] }, () => {",
      '  beforeAll(async () => {',
      '    await ddb.send(new PutItemCommand({',
      '      AttributesToGet: [x],',
      '    }))',
      '  })',
      "  it('unrelated', async () => {",
      '    expect(1).toBe(1)',
      '  })',
      '})',
      '',
    ].join('\n')
    expect(tags(capabilityLeaks(src))).toEqual(['describe:PutItem:legacy'])
  })

  it('ignores a legacy parameter name inside an asserted message', () => {
    const src = [
      "describe('BatchWriteItem', { tags: ['batch', 'data-plane'] }, () => {",
      "  it('type mismatch on an index key', async () => {",
      '    expect(err.message).toContain(',
      "      'One or more parameter values were invalid: Type mismatch for Index Key lsi1sk Expected: S Actual: N',",
      '    )',
      '  })',
      '})',
      '',
    ].join('\n')
    expect(capabilityLeaks(src)).toEqual([])
  })

  it('ignores AttributesToGet named in a Select rejection message', () => {
    const src = [
      "describe('Query', { tags: ['query', 'data-plane'] }, () => {",
      "  it('SPECIFIC_ATTRIBUTES without a projection', async () => {",
      '    expect(err.message).toContain(',
      "      'Must specify the AttributesToGet or ProjectionExpression when choosing to get SPECIFIC_ATTRIBUTES',",
      '    )',
      '  })',
      '})',
      '',
    ].join('\n')
    expect(capabilityLeaks(src)).toEqual([])
  })

  it('catches a legacy parameter written inline rather than on its own line', () => {
    const src = [
      "describe('GetItem', { tags: ['get-item', 'data-plane'] }, () => {",
      "  it('inline', async () => {",
      '    await ddb.send(new GetItemCommand({ AttributesToGet: [pk] }))',
      '  })',
      '})',
      '',
    ].join('\n')
    expect(tags(capabilityLeaks(src))).toEqual(['test:inline:legacy'])
  })

  it('does not match a longer identifier that ends with a parameter name', () => {
    const src = [
      "describe('GetItem', { tags: ['get-item', 'data-plane'] }, () => {",
      "  it('unrelated', async () => {",
      '    const myExpected: string = x',
      '    expect(myExpected).toBe(x)',
      '  })',
      '})',
      '',
    ].join('\n')
    expect(capabilityLeaks(src)).toEqual([])
  })

  it('attributes a marker to the right test when tests open and close on one line', () => {
    // A one-line test is not closed until the following line, so its recorded
    // range overlaps the next test's opening. The marker must still land on the
    // test that sends it, not on the one before.
    const src = [
      "describe('GetItem', { tags: ['get-item', 'data-plane'] }, () => {",
      "  it('first', async () => { expect(1).toBe(1) })",
      "  it('second', async () => { await ddb.send(new GetItemCommand({ AttributesToGet: [pk] })) })",
      "  it('third', async () => { expect(1).toBe(1) })",
      '})',
      '',
    ].join('\n')
    expect(tags(capabilityLeaks(src))).toEqual(['test:second:legacy'])
  })

  it('reports a PartiQL command in a describe not tagged partiql', () => {
    const src = [
      "describe('Statements', { tags: ['data-plane'] }, () => {",
      "  it('runs one', async () => {",
      '    await ddb.send(new ExecuteStatementCommand({ Statement: s }))',
      '  })',
      '})',
      '',
    ].join('\n')
    expect(tags(capabilityLeaks(src))).toEqual(['test:runs one:partiql'])
  })

  it('does not count naming a command in an import as sending it', () => {
    const src = [
      "import { ExecuteStatementCommand } from '@aws-sdk/client-dynamodb'",
      "describe('Something else', { tags: ['scan', 'data-plane'] }, () => {",
      "  it('unrelated', async () => {",
      '    expect(1).toBe(1)',
      '  })',
      '})',
      '',
    ].join('\n')
    expect(capabilityLeaks(src)).toEqual([])
  })

  it('resolves tags from the whole enclosing chain, not just the top describe', () => {
    const src = [
      "describe('Outer', { tags: ['put-item', 'data-plane'] }, () => {",
      "  describe('Inner', { tags: ['legacy'] }, () => {",
      "    it('deep', async () => {",
      '      Expected: { pk: { Exists: false } },',
      '    })',
      '  })',
      '})',
      '',
    ].join('\n')
    expect(capabilityLeaks(src)).toEqual([])
  })

  it('reports an index-key attribute written by an untagged test', () => {
    const src = [
      "describe('PutItem', { tags: ['put-item', 'data-plane'] }, () => {",
      "  it('rejects a wrong-typed index key', async () => {",
      '    await ddb.send(new PutItemCommand({ Item: { pk: p, lsi1sk: { N: :v } } }))',
      '  })',
      '})',
      '',
    ].join('\n')
    expect(tags(capabilityLeaks(src))).toEqual(['test:rejects a wrong-typed index key:gsi or lsi'])
  })

  it('accepts either index axis for an index-key attribute', () => {
    const withLsi = [
      "describe('PutItem', { tags: ['put-item', 'data-plane', 'lsi'] }, () => {",
      "  it('a', async () => { await ddb.send(new PutItemCommand({ Item: { lsi1sk: v } })) })",
      '})',
      '',
    ].join('\n')
    const withGsi = withLsi.replace("'lsi'", "'gsi'")
    expect(capabilityLeaks(withLsi)).toEqual([])
    expect(capabilityLeaks(withGsi)).toEqual([])
  })

  it('does not treat an index-key name inside an asserted message as a dependency', () => {
    const src = [
      "describe('Scan', { tags: ['scan', 'data-plane'] }, () => {",
      "  it('reports the message', async () => {",
      "    expect(err.message).toBe('Type mismatch for Index Key lsi1sk: Expected S')",
      '  })',
      '})',
      '',
    ].join('\n')
    expect(capabilityLeaks(src)).toEqual([])
  })

  it('requires the tag on every top-level describe when the marker is at module scope', () => {
    const src = [
      'const legacyRead = { AttributesToGet: [pk] }',
      "describe('Tagged', { tags: ['get-item', 'legacy', 'data-plane'] }, () => {",
      "  it('a', async () => { expect(1).toBe(1) })",
      '})',
      "describe('Untagged', { tags: ['get-item', 'data-plane'] }, () => {",
      "  it('b', async () => { expect(1).toBe(1) })",
      '})',
      '',
    ].join('\n')
    expect(tags(capabilityLeaks(src))).toEqual(['file:Untagged:legacy'])
  })
})

describe('the marker table', () => {
  // tag-content.mjs cannot import src/tags.ts, because scripts/tag-manifest.mjs
  // consumes it under plain node. Asserting the table here is what keeps a
  // typo'd tag name from silently checking nothing.
  it('only requires tags that exist in the canonical vocabulary', () => {
    const undeclared = CAPABILITY_MARKERS.map((m) => m.tag).filter((t) => !TAG_NAMES.has(t))
    expect(undeclared, 'marker table references tags absent from src/tags.ts').toEqual([])
  })

  it('describes what each marker matches, for the failure message', () => {
    for (const m of CAPABILITY_MARKERS) {
      expect(m.what, `marker for '${m.tag}' has no description`).toBeTruthy()
    }
  })
})
