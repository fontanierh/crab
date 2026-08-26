const MAX_LINE_BYTES = 16 * 1024 * 1024;
const CALL_TIMEOUT_MS = 30_000;

function safeError(id, code = 'PackageRejected') {
  return {
    jsonrpc: '2.0',
    id,
    error: { code, message: 'bridge request rejected' },
  };
}

export class ProtocolPeer {
  constructor({ input, output, service, callTimeoutMs = CALL_TIMEOUT_MS }) {
    this.input = input;
    this.output = output;
    this.service = service;
    this.callTimeoutMs = callTimeoutMs;
    this.buffer = '';
    this.sequence = 0;
    this.pending = new Map();
    this.closed = false;
  }

  start() {
    this.input.setEncoding('utf8');
    this.input.on('data', (chunk) => this.#read(chunk));
    this.input.on('end', () => this.close());
    this.input.on('error', () => this.close());
  }

  async callHost(method, params) {
    if (this.closed) throw new Error('protocol closed');
    const id = `whatsapp-${++this.sequence}`;
    const result = new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error('host call timed out'));
      }, this.callTimeoutMs);
      timer.unref?.();
      this.pending.set(id, { resolve, reject, timer });
    });
    this.#write({ jsonrpc: '2.0', id, method, params });
    return result;
  }

  close() {
    if (this.closed) return;
    this.closed = true;
    for (const pending of this.pending.values()) {
      clearTimeout(pending.timer);
      pending.reject(new Error('protocol closed'));
    }
    this.pending.clear();
  }

  #read(chunk) {
    this.buffer += chunk;
    if (Buffer.byteLength(this.buffer) > MAX_LINE_BYTES && !this.buffer.includes('\n')) {
      this.close();
      return;
    }
    while (true) {
      const newline = this.buffer.indexOf('\n');
      if (newline < 0) return;
      const line = this.buffer.slice(0, newline);
      this.buffer = this.buffer.slice(newline + 1);
      if (Buffer.byteLength(line) > MAX_LINE_BYTES) {
        this.close();
        return;
      }
      this.#handleLine(line);
    }
  }

  #handleLine(line) {
    let message;
    try {
      message = JSON.parse(line);
    } catch {
      this.close();
      return;
    }
    if (typeof message.method === 'string') {
      void this.#handleRequest(message);
      return;
    }
    const pending = this.pending.get(message.id);
    if (!pending) return;
    this.pending.delete(message.id);
    clearTimeout(pending.timer);
    if (message.error) pending.reject(new Error('host rejected package call'));
    else if (Object.hasOwn(message, 'result')) pending.resolve(message.result);
    else pending.reject(new Error('invalid host response'));
  }

  async #handleRequest(message) {
    const method = {
      'bridge/initialize': 'initialize',
      'bridge/health': 'health',
      'bridge/auth/begin': 'beginAuthentication',
      'bridge/auth/submit': 'submitAuthentication',
      'bridge/auth/validate': 'validateCredentials',
      'bridge/auth/committed': 'credentialCommitted',
      'bridge/auth/invalidate': 'invalidateCredentials',
      'bridge/deliver': 'deliver',
      'bridge/shutdown': 'shutdown',
    }[message.method];
    if (!method || typeof this.service[method] !== 'function') {
      this.#write(safeError(message.id, 'MethodNotFound'));
      return;
    }
    try {
      const result = await this.service[method](message.params ?? {});
      this.#write({ jsonrpc: '2.0', id: message.id, result: result ?? {} });
    } catch {
      this.#write(safeError(message.id));
    }
  }

  #write(message) {
    this.output.write(`${JSON.stringify(message)}\n`);
  }
}
