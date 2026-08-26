import { EventEmitter } from 'node:events';

export const bufferJson = {
  replacer(_key, value) {
    return value;
  },
  reviver(_key, value) {
    if (value?.type === 'Buffer' && Array.isArray(value.data)) {
      return Buffer.from(value.data);
    }
    return value;
  },
};

export function credential(registered = true) {
  return {
    schemaVersion: 1,
    creds: { registered, me: registered ? { id: 'agent@s.whatsapp.net' } : null },
    keys: {},
  };
}

export class FakeSocket {
  constructor(auth) {
    this.auth = auth;
    this.ev = new EventEmitter();
    this.user = { id: 'agent@s.whatsapp.net' };
    this.sent = [];
    this.ended = false;
  }

  async requestPairingCode() {
    return '1234-5678';
  }

  async sendMessage(chatId, content, options) {
    this.sent.push({ chatId, content, options });
    return { key: { id: options.messageId } };
  }

  end() {
    this.ended = true;
  }
}

export function dependencies(overrides = {}) {
  const sockets = [];
  return {
    sockets,
    options: {
      socketFactory: async ({ auth }) => {
        const socket = new FakeSocket(auth);
        sockets.push(socket);
        return socket;
      },
      initAuthCreds: () => ({ registered: false, me: null }),
      bufferJson,
      appStateSyncKeyFromObject: (value) => ({ ...value, revived: true }),
      disconnectStatus: (error) => error?.status ?? null,
      loggedOutStatus: 401,
      authTimeoutMs: 1_000,
      now: () => 10_000,
      ...overrides,
    },
  };
}

export function flush() {
  return new Promise((resolve) => setImmediate(resolve));
}
