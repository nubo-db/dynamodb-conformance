// How DynamoDB measures an item against the 400KB gate, and how to build a
// fixture that lands on it exactly.
//
// Captured against eu-west-2 on 2026-08-18. The gate itself resolves to the
// byte - 409,600 accepted, 409,601 refused - which is what makes it a usable
// instrument: consumed capacity is 1KB-granular and far too coarse to see a
// single attribute's cost, but an accept/reject pair either side of the gate
// measures one to the byte with two requests and no bisection.
//
// Kept free of any AWS import beyond the value type so it runs in the tooling
// lane, where its own unit test pins the number formula against the twenty-three
// literals the capture measured.

import type { AttributeValue } from '@aws-sdk/client-dynamodb'

/**
 * The item-size gate. An item of exactly this many bytes is accepted and one
 * byte more is refused, so the comparison DynamoDB makes is `size > 409600`.
 */
export const MAX_ITEM_BYTES = 409_600

/** UTF-8 length, which is what DynamoDB counts for a name or a string value. */
export function utf8Bytes(text: string): number {
  return Buffer.byteLength(text, 'utf8')
}

/**
 * The twenty-three literals measured against eu-west-2 on 2026-08-18, each by an
 * accept/reject pair either side of the 400KB gate.
 *
 * This is the capture record, not a derivation: `numberBytes` is checked against
 * it in the tooling lane, and tests/tier3/limits/numberByteSize.test.ts builds
 * its fixtures from the byte figures here rather than from the formula, so a
 * mistake in the formula cannot move the prediction and the assertion together.
 */
export const CAPTURED_NUMBER_BYTES: readonly (readonly [literal: string, bytes: number, why: string])[] = [
  ['0', 1, 'zero'],
  ['1', 2, ''],
  ['12', 2, 'two digits cost what one does'],
  ['123', 3, ''],
  ['1234', 3, 'odd counts round up, so 3 and 4 digits cost the same'],
  ['12345678901234567890123456789012345678', 20, '38 digits, the precision ceiling'],
  ['0042', 2, 'leading zeros trimmed'],
  ['100', 2, 'trailing zeros trimmed'],
  ['1010', 3, 'the interior zero is significant, the trailing one is not'],
  ['0.0000001', 2, "the fraction's leading zeros are not significant"],
  ['1E125', 2, 'the exponent is not counted'],
  ['1E-100', 2, 'nor a negative one'],
  ['1.5', 3, 'straddles the point, so costs a byte more than 15'],
  ['15', 2, 'the same two digits without a straddle'],
  ['1.2', 3, ''],
  ['1.200', 3, "the fraction's trailing zeros are not significant"],
  ['1.234', 4, ''],
  ['3.14159', 5, 'the documented rule predicts 4'],
  ['123456', 4, 'the same digit count as 3.14159, without a straddle'],
  ['100.5', 4, "the integer's trailing zeros count once a fraction follows them"],
  ['0.15', 2, 'no integer digits, so no straddle'],
  ['-42', 3, 'the sign byte is real'],
  ['-0', 1, 'zero is never negative, whatever the literal said'],
]

/**
 * A number's byte cost:
 *
 * ```
 * 1 + ceil(integer significant digits / 2) + ceil(fraction significant digits / 2)
 *   + 1 if negative
 * ```
 *
 * over the value with leading and trailing zeros trimmed. A pure fraction
 * contributes zero for the integer half, and zero is never negative whatever the
 * literal said.
 *
 * The rule the AWS developer guide documents - "1 byte per two significant
 * digits plus 1 byte" - is wrong in two ways, and an implementation written from
 * it lands on exactly those. It rounds down where AWS rounds up, and it does not
 * account for significant digits straddling the decimal point, which are counted
 * in two halves and so cost an extra byte: `3.14159` costs 5 where the documented
 * rule predicts 4, and the integer `123456` with the same digit count costs 4.
 *
 * The exponent is not counted. A number is sized by its significant digits, so
 * the figure does not move when storage expands `1E125` to 126 digits.
 */
export function numberBytes(literal: string): number {
  const parsed = /^([+-]?)(\d*)(?:\.(\d*))?(?:[eE]([+-]?\d+))?$/.exec(literal.trim())
  if (!parsed || (!parsed[2] && !parsed[3])) {
    throw new Error(`not a DynamoDB number literal: ${literal}`)
  }
  const [, sign, intText, fracText = '', expText] = parsed
  const digits = `${intText}${fracText}`
  // No non-zero digit anywhere is zero, however it was spelled. `-0` costs 1.
  if (!/[1-9]/.test(digits)) return 1

  // Where the decimal point sits in `digits` once the exponent is applied.
  const point = intText.length + Number(expText ?? 0)
  let intPart: string
  let fracPart: string
  if (point <= 0) {
    intPart = ''
    fracPart = '0'.repeat(-point) + digits
  } else if (point >= digits.length) {
    intPart = digits + '0'.repeat(point - digits.length)
    fracPart = ''
  } else {
    intPart = digits.slice(0, point)
    fracPart = digits.slice(point)
  }

  intPart = intPart.replace(/^0+/, '')
  fracPart = fracPart.replace(/0+$/, '')

  // A zero between two significant digits is itself significant, so trailing
  // zeros only fall off the integer half when no fraction follows them: `100`
  // costs 2 and `1010` costs 3, but `100.5` counts all three integer digits.
  const intSignificant = fracPart === '' ? intPart.replace(/0+$/, '').length : intPart.length
  const fracSignificant = intPart === '' ? fracPart.replace(/^0+/, '').length : fracPart.length

  return (
    1 +
    Math.ceil(intSignificant / 2) +
    Math.ceil(fracSignificant / 2) +
    (sign === '-' ? 1 : 0)
  )
}

/**
 * One value's contribution to the item size.
 *
 * Deliberately narrow. The captures settle S, N and B, so those are what this
 * claims; a document type reaches the `throw` rather than a guess that would
 * read as ground truth.
 */
export function attributeValueBytes(value: AttributeValue): number {
  if (value.S !== undefined) return utf8Bytes(value.S)
  if (value.N !== undefined) return numberBytes(value.N)
  if (value.B !== undefined) return (value.B as Uint8Array).byteLength
  throw new Error(`no captured sizing rule for ${Object.keys(value).join(',')}`)
}

/** An item's size: every attribute's name plus its value. */
export function itemBytes(item: Record<string, AttributeValue>): number {
  return Object.entries(item).reduce(
    (total, [name, value]) => total + utf8Bytes(name) + attributeValueBytes(value),
    0,
  )
}

/**
 * An ASCII string of exactly `length` bytes.
 *
 * The assertion is the point. The most expensive false start in the capture was
 * a value built by repeating a seed and truncating, which silently produced 504
 * bytes where 1,000 was asked for; every measurement taken against it was then
 * wrong in a way that read as a finding about DynamoDB rather than about the
 * fixture. Anything building a value to a target length checks it got one.
 */
export function asciiOfBytes(length: number): string {
  if (!Number.isInteger(length) || length < 0) {
    throw new Error(`cannot build a string of ${length} bytes`)
  }
  const built = 'x'.repeat(length)
  if (utf8Bytes(built) !== length) {
    throw new Error(`built ${utf8Bytes(built)} bytes, asked for ${length}`)
  }
  return built
}

/**
 * `base` plus one padding attribute, sized so the whole item measures exactly
 * `target` bytes. Throws rather than returning something close.
 */
export function itemOfBytes(
  target: number,
  base: Record<string, AttributeValue>,
  padAttribute = 'p',
): Record<string, AttributeValue> {
  const padLength = target - itemBytes(base) - utf8Bytes(padAttribute)
  if (padLength < 0) {
    throw new Error(
      `${itemBytes(base) + utf8Bytes(padAttribute)} bytes of fixture will not fit in ${target}`,
    )
  }
  const item = { ...base, [padAttribute]: { S: asciiOfBytes(padLength) } }
  const measured = itemBytes(item)
  if (measured !== target) {
    throw new Error(`built an item of ${measured} bytes, asked for ${target}`)
  }
  return item
}
