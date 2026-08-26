import { createHash } from 'node:crypto';

export function canonicalJson(value) {
  if (value === null || typeof value !== 'object') {
    const encoded = JSON.stringify(value);
    return encoded === undefined ? 'null' : encoded;
  }
  if (Array.isArray(value)) {
    return `[${value.map((entry) => canonicalJson(entry)).join(',')}]`;
  }
  const entries = Object.keys(value)
    .filter((key) => value[key] !== undefined)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${canonicalJson(value[key])}`);
  return `{${entries.join(',')}}`;
}

export function credentialFingerprint(value) {
  return createHash('sha256').update(canonicalJson(value)).digest('hex');
}

export function outboundMessageId(idempotencyKey) {
  const digest = createHash('sha256').update(idempotencyKey).digest('hex').toUpperCase();
  return `3EB0${digest.slice(0, 18)}`;
}
