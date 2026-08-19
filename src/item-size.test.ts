import { describe, it, expect } from 'vitest'
import {
  MAX_ITEM_BYTES,
  numberBytes,
  attributeValueBytes,
  itemBytes,
  asciiOfBytes,
  itemOfBytes,
  utf8Bytes,
  CAPTURED_NUMBER_BYTES,
} from './item-size.js'

describe('numberBytes', () => {
  // Checked against the capture record rather than against the formula's own
  // reasoning: every literal here was measured, not predicted.
  for (const [literal, bytes, why] of CAPTURED_NUMBER_BYTES) {
    it(`sizes ${literal} at ${bytes}${why ? ` — ${why}` : ''}`, () => {
      expect(numberBytes(literal)).toBe(bytes)
    })
  }

  // The two places the AWS developer guide's rule ("1 byte per two significant
  // digits plus 1 byte") diverges from what AWS does. An implementation written
  // from the documentation lands on exactly these, so they are worth naming.
  it('rounds up where the documented rule rounds down', () => {
    expect(numberBytes('123')).toBe(3)
    expect(numberBytes('1234')).toBe(3)
  })

  it('charges a byte for digits straddling the decimal point', () => {
    expect(numberBytes('3.14159') - numberBytes('314159')).toBe(1)
    expect(numberBytes('1.5') - numberBytes('15')).toBe(1)
  })

  it('sizes a negative at one byte more than its magnitude, except at zero', () => {
    for (const literal of ['42', '12345', '1E125', '0.15']) {
      expect(numberBytes(`-${literal}`) - numberBytes(literal), literal).toBe(1)
    }
    expect(numberBytes('-0')).toBe(numberBytes('0'))
  })

  it('rejects something that is not a number literal', () => {
    expect(() => numberBytes('abc')).toThrow(/not a DynamoDB number literal/)
    expect(() => numberBytes('')).toThrow(/not a DynamoDB number literal/)
  })
})

describe('attributeValueBytes', () => {
  it('sizes a string by its UTF-8 length', () => {
    expect(attributeValueBytes({ S: 'abc' })).toBe(3)
    expect(attributeValueBytes({ S: '£' })).toBe(2)
  })

  it('sizes binary by its raw bytes', () => {
    expect(attributeValueBytes({ B: new Uint8Array(17) })).toBe(17)
  })

  it('sizes a number by the formula', () => {
    expect(attributeValueBytes({ N: '3.14159' })).toBe(5)
  })

  // The captures settle S, N and B. Anything else would be a guess dressed up as
  // ground truth, so it fails loudly instead.
  it('refuses a type no capture measured', () => {
    expect(() => attributeValueBytes({ BOOL: true })).toThrow(/no captured sizing rule/)
    expect(() => attributeValueBytes({ L: [{ S: 'a' }] })).toThrow(/no captured sizing rule/)
  })
})

describe('itemBytes', () => {
  it('counts attribute names as well as values', () => {
    expect(itemBytes({ pk: { S: 'k' } })).toBe(3)
    expect(itemBytes({ pk: { S: 'k' }, n: { N: '42' } })).toBe(3 + 1 + 2)
  })

  it('counts a multi-byte attribute name in bytes, not characters', () => {
    expect(itemBytes({ '£': { S: '' } })).toBe(2)
  })
})

describe('asciiOfBytes', () => {
  it('builds a string of exactly the requested length', () => {
    expect(utf8Bytes(asciiOfBytes(1_000))).toBe(1_000)
    expect(asciiOfBytes(0)).toBe('')
  })

  it('refuses a length it cannot build', () => {
    expect(() => asciiOfBytes(-1)).toThrow(/cannot build a string/)
    expect(() => asciiOfBytes(1.5)).toThrow(/cannot build a string/)
  })
})

describe('itemOfBytes', () => {
  it('lands on the target exactly, gate included', () => {
    const item = itemOfBytes(MAX_ITEM_BYTES, { pk: { S: 'k' } })
    expect(itemBytes(item)).toBe(MAX_ITEM_BYTES)
  })

  it('accounts for the padding attribute name it adds', () => {
    const item = itemOfBytes(100, { pk: { S: 'k' } }, 'padding')
    expect(itemBytes(item)).toBe(100)
    expect(item.padding.S).toHaveLength(100 - 3 - 'padding'.length)
  })

  it('refuses a target the fixture already overruns', () => {
    expect(() => itemOfBytes(3, { pk: { S: 'kkkk' } })).toThrow(/will not fit/)
  })
})
