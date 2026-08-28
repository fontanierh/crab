import assert from 'node:assert/strict';
import test from 'node:test';

import {
  MAX_CREDENTIAL_SNAPSHOT_BYTES,
  createAuthState,
  validCredentialSnapshot,
} from '../src/auth-state.js';
import { bufferJson, credential } from './helpers.js';

function credentialWithEncodedSize(size) {
  const snapshot = credential();
  snapshot.keys.padding = '';
  const fixedBytes = Buffer.byteLength(JSON.stringify(snapshot));
  assert.ok(size >= fixedBytes);
  snapshot.keys.padding = 'x'.repeat(size - fixedBytes);
  assert.equal(Buffer.byteLength(JSON.stringify(snapshot)), size);
  return snapshot;
}

test('auth snapshots retain signal keys across a restart and report each mutation', async () => {
  let mutations = 0;
  const first = createAuthState({
    snapshot: credential(),
    initAuthCreds: () => ({ registered: false }),
    bufferJson,
    appStateSyncKeyFromObject: (value) => ({ ...value, revived: true }),
  });
  first.onMutation(async () => { mutations += 1; });
  await first.state.keys.set({
    session: { alice: Buffer.from('signal-key') },
    'app-state-sync-key': { sync: { keyData: 'one' } },
  });
  assert.equal(mutations, 1);

  const snapshot = first.snapshot();
  const restarted = createAuthState({
    snapshot,
    initAuthCreds: () => ({ registered: false }),
    bufferJson,
    appStateSyncKeyFromObject: (value) => ({ ...value, revived: true }),
  });
  const session = await restarted.state.keys.get('session', ['alice']);
  const appState = await restarted.state.keys.get('app-state-sync-key', ['sync']);
  assert.equal(session.alice.toString(), 'signal-key');
  assert.deepEqual(appState.sync, { keyData: 'one', revived: true });
  assert.equal(validCredentialSnapshot(snapshot), true);
});

test('malformed or unregistered snapshots fail closed', () => {
  assert.equal(validCredentialSnapshot(null), false);
  assert.equal(validCredentialSnapshot({ schemaVersion: 2, creds: {}, keys: {} }), false);
  assert.equal(validCredentialSnapshot(credential(false)), false);
});

test('credential snapshots accept the exact byte ceiling and reject one byte more', () => {
  const maximum = credentialWithEncodedSize(MAX_CREDENTIAL_SNAPSHOT_BYTES);
  const oversized = credentialWithEncodedSize(MAX_CREDENTIAL_SNAPSHOT_BYTES + 1);

  assert.equal(validCredentialSnapshot(maximum), true);
  const restored = createAuthState({
    snapshot: maximum,
    initAuthCreds: () => ({ registered: false }),
    bufferJson,
    appStateSyncKeyFromObject: (value) => value,
  });
  assert.equal(
    Buffer.byteLength(JSON.stringify(restored.snapshot())),
    MAX_CREDENTIAL_SNAPSHOT_BYTES,
  );
  assert.equal(validCredentialSnapshot(oversized), false);
  assert.throws(() => createAuthState({
    snapshot: oversized,
    initAuthCreds: () => ({ registered: false }),
    bufferJson,
    appStateSyncKeyFromObject: (value) => value,
  }), /snapshot too large/);
});

test('live key growth fails before an oversized snapshot can be published', async () => {
  const auth = createAuthState({
    snapshot: credential(),
    initAuthCreds: () => ({ registered: false }),
    bufferJson,
    appStateSyncKeyFromObject: (value) => value,
  });
  await auth.state.keys.set({
    session: { oversized: 'x'.repeat(MAX_CREDENTIAL_SNAPSHOT_BYTES) },
  });
  assert.throws(() => auth.snapshot(), /snapshot too large/);
});
