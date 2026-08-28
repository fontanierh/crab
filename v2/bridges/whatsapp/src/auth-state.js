const SNAPSHOT_SCHEMA = 1;
export const MAX_CREDENTIAL_SNAPSHOT_BYTES = 8 * 1024 * 1024;

function jsonRoundTrip(value, replacer, reviver) {
  return JSON.parse(JSON.stringify(value, replacer), reviver);
}

function assertSnapshot(snapshot) {
  if (
    snapshot === null ||
    typeof snapshot !== 'object' ||
    Array.isArray(snapshot) ||
    snapshot.schemaVersion !== SNAPSHOT_SCHEMA ||
    snapshot.creds === null ||
    typeof snapshot.creds !== 'object' ||
    Array.isArray(snapshot.creds) ||
    snapshot.keys === null ||
    typeof snapshot.keys !== 'object' ||
    Array.isArray(snapshot.keys)
  ) {
    throw new Error('invalid credential snapshot');
  }
  let encoded;
  try {
    encoded = JSON.stringify(snapshot);
  } catch {
    throw new Error('invalid credential snapshot');
  }
  if (
    typeof encoded !== 'string' ||
    Buffer.byteLength(encoded) > MAX_CREDENTIAL_SNAPSHOT_BYTES
  ) {
    throw new Error('credential snapshot too large');
  }
}

export function createAuthState({ snapshot, initAuthCreds, bufferJson, appStateSyncKeyFromObject }) {
  if (snapshot !== null) assertSnapshot(snapshot);
  const decoded = snapshot === null
    ? { schemaVersion: SNAPSHOT_SCHEMA, creds: initAuthCreds(), keys: {} }
    : jsonRoundTrip(snapshot, undefined, bufferJson.reviver);
  assertSnapshot(decoded);

  let mutationHandler = async () => {};
  const keyData = decoded.keys;
  const state = {
    creds: decoded.creds,
    keys: {
      async get(type, ids) {
        const result = {};
        for (const id of ids) {
          let value = keyData[type]?.[id];
          if (type === 'app-state-sync-key' && value) {
            value = appStateSyncKeyFromObject(value);
          }
          result[id] = value;
        }
        return result;
      },
      async set(changes) {
        for (const [type, entries] of Object.entries(changes)) {
          keyData[type] ??= {};
          for (const [id, value] of Object.entries(entries)) {
            if (value === null || value === undefined) {
              delete keyData[type][id];
            } else {
              keyData[type][id] = value;
            }
          }
          if (Object.keys(keyData[type]).length === 0) {
            delete keyData[type];
          }
        }
        await mutationHandler();
      },
    },
  };

  return {
    state,
    snapshot() {
      const encoded = jsonRoundTrip(
        { schemaVersion: SNAPSHOT_SCHEMA, creds: state.creds, keys: keyData },
        bufferJson.replacer,
        undefined,
      );
      assertSnapshot(encoded);
      return encoded;
    },
    onMutation(handler) {
      mutationHandler = handler;
    },
  };
}

export function validCredentialSnapshot(snapshot) {
  try {
    assertSnapshot(snapshot);
    return snapshot.creds.registered === true;
  } catch {
    return false;
  }
}
