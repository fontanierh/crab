import assert from 'node:assert/strict';
import test from 'node:test';

import { createAuthState, validCredentialSnapshot } from '../src/auth-state.js';
import { bufferJson, credential } from './helpers.js';

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
