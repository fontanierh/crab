import { EventEmitter } from 'node:events';

import { createAuthState, validCredentialSnapshot } from './auth-state.js';
import { credentialFingerprint, outboundMessageId } from './canonical-json.js';
import {
  inboundAllowed,
  inboundPolicySummary,
  parseInboundPolicy,
} from './inbound-policy.js';
import { MAX_MEDIA_BYTES } from './media-policy.js';
import { mediaDescriptor, normalizeInbound } from './message.js';
import { outboundContent } from './outbound-media.js';

const PROTOCOL_VERSION = 2;
const DEFAULT_AUTH_TIMEOUT_MS = 120_000;

function object(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function delay(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

export class CredentialPublisher {
  constructor({ bridgeId, snapshot, callHost, onFailure }) {
    this.bridgeId = bridgeId;
    this.snapshot = snapshot;
    this.callHost = callHost;
    this.onFailure = onFailure;
    this.enabled = false;
    this.previousFingerprint = null;
    this.tail = Promise.resolve();
  }

  restore(committedSnapshot) {
    this.previousFingerprint = credentialFingerprint(committedSnapshot);
    this.enabled = true;
  }

  async commit(committedSnapshot) {
    this.restore(committedSnapshot);
    await this.changed();
  }

  changed() {
    if (!this.enabled) return Promise.resolve();
    const operation = this.tail.then(async () => {
      const credential = this.snapshot();
      const nextFingerprint = credentialFingerprint(credential);
      if (nextFingerprint === this.previousFingerprint) return;
      const receipt = await this.callHost('bridge/credential/update', {
        bridgeId: this.bridgeId,
        previousFingerprint: this.previousFingerprint,
        credential,
      });
      if (receipt?.credentialFingerprint !== nextFingerprint) {
        throw new Error('credential acknowledgement mismatch');
      }
      this.previousFingerprint = nextFingerprint;
    });
    this.tail = operation.catch((error) => this.onFailure(error));
    return operation;
  }
}

export class WhatsAppBridgeService {
  constructor({
    socketFactory,
    initAuthCreds,
    bufferJson,
    appStateSyncKeyFromObject,
    disconnectStatus,
    loggedOutStatus,
    downloadMedia,
    callHost,
    onFatal,
    authTimeoutMs = DEFAULT_AUTH_TIMEOUT_MS,
    now = Date.now,
  }) {
    this.socketFactory = socketFactory;
    this.authDependencies = { initAuthCreds, bufferJson, appStateSyncKeyFromObject };
    this.disconnectStatus = disconnectStatus;
    this.loggedOutStatus = loggedOutStatus;
    this.downloadMedia = downloadMedia;
    this.callHost = callHost;
    this.onFatal = onFatal;
    this.authTimeoutMs = authTimeoutMs;
    this.now = now;
    this.events = new EventEmitter();
    this.initialized = false;
    this.connected = false;
    this.connection = 'idle';
    this.bridgeId = null;
    this.targetChannelId = null;
    this.browserName = 'Crab';
    this.inboundPolicy = parseInboundPolicy(undefined);
    this.socket = null;
    this.socketGeneration = 0;
    this.auth = null;
    this.publisher = null;
    this.lastQr = null;
    this.reconnectTimer = null;
    this.reconnectAttempts = 0;
    this.credentialRejected = false;
    this.shuttingDown = false;
    this.inboundTail = Promise.resolve();
  }

  async initialize(params) {
    if (
      this.initialized ||
      params.protocolVersion !== PROTOCOL_VERSION ||
      typeof params.bridgeId !== 'string' ||
      params.bridgeId.trim() === '' ||
      typeof params.packageId !== 'string' ||
      !object(params.configuration) ||
      typeof params.configuration.targetChannelId !== 'string' ||
      params.configuration.targetChannelId.trim() === ''
    ) {
      throw new Error('invalid initialization');
    }
    const allowed = new Set(['targetChannelId', 'browserName', 'inboundPolicy']);
    if (Object.keys(params.configuration).some((key) => !allowed.has(key))) {
      throw new Error('unknown configuration');
    }
    this.bridgeId = params.bridgeId;
    this.targetChannelId = params.configuration.targetChannelId;
    this.inboundPolicy = parseInboundPolicy(params.configuration.inboundPolicy);
    if (params.configuration.browserName !== undefined) {
      if (
        typeof params.configuration.browserName !== 'string' ||
        params.configuration.browserName.trim() === ''
      ) {
        throw new Error('invalid browser name');
      }
      this.browserName = params.configuration.browserName;
    }
    this.initialized = true;
    return { protocolVersion: PROTOCOL_VERSION };
  }

  async health(params) {
    this.#requireInitialized();
    if (params.credential !== null && params.credential !== undefined && !this.auth) {
      if (validCredentialSnapshot(params.credential)) {
        this.#prepareAuth(params.credential, true);
        await this.#connect();
      }
    }
    const credentialValid = !this.credentialRejected && (params.credential !== null && params.credential !== undefined
      ? validCredentialSnapshot(params.credential)
      : Boolean(this.publisher?.enabled && validCredentialSnapshot(this.auth?.snapshot())));
    return {
      processAlive: !this.shuttingDown,
      serviceConnected: this.connected,
      canReceive: this.connected,
      canSend: this.connected,
      credentialValid,
      detail: {
        connection: this.connection,
        pairingAvailable: !this.connected,
        inboundPolicy: inboundPolicySummary(this.inboundPolicy),
      },
    };
  }

  async beginAuthentication(params) {
    this.#requireInitialized();
    if (this.connected) throw new Error('already connected');
    const method = params.method ?? 'qrCode';
    if (method !== 'qrCode' && method !== 'phoneCode') {
      throw new Error('unsupported authentication method');
    }
    if (!object(params.context)) throw new Error('invalid authentication context');
    await this.#stopSocket();
    this.#prepareAuth(null, false);
    await this.#connect();
    if (method === 'phoneCode') {
      const phoneNumber = String(params.context.phoneNumber ?? '').replace(/\D/g, '');
      if (phoneNumber.length < 8 || phoneNumber.length > 15) {
        throw new Error('invalid phone number');
      }
      const code = await this.socket.requestPairingCode(phoneNumber);
      return {
        method,
        expiresAtMs: this.now() + this.authTimeoutMs,
        presentation: { kind: 'phoneCode', code },
      };
    }
    const qr = this.lastQr ?? await this.#waitFor('qr', () => this.lastQr);
    return {
      method,
      expiresAtMs: this.now() + this.authTimeoutMs,
      presentation: { kind: 'qrCode', value: qr },
    };
  }

  async submitAuthentication(params) {
    this.#requireInitialized();
    if (typeof params.challengeId !== 'string' || !object(params.response) || !this.auth) {
      throw new Error('invalid authentication submission');
    }
    if (!this.connected) {
      await this.#waitFor('connected', () => this.connected);
    }
    const credential = this.auth.snapshot();
    if (!validCredentialSnapshot(credential)) {
      throw new Error('credentials are not registered');
    }
    return {
      credential,
      expiresAtMs: null,
      accountHint: this.socket?.user?.id ?? null,
      detail: { paired: true },
    };
  }

  async validateCredentials(params) {
    this.#requireInitialized();
    const valid = !this.credentialRejected && validCredentialSnapshot(params.credential);
    return {
      valid,
      expiresAtMs: null,
      accountHint: valid ? (this.socket?.user?.id ?? null) : null,
      detail: { registered: valid },
    };
  }

  async credentialCommitted(params) {
    this.#requireInitialized();
    if (!this.publisher || !validCredentialSnapshot(params.credential)) {
      throw new Error('invalid committed credential');
    }
    await this.publisher.commit(params.credential);
    return {};
  }

  async invalidateCredentials(params) {
    this.#requireInitialized();
    if (!object(params.credential)) throw new Error('invalid credential');
    await this.#stopSocket();
    this.auth = null;
    this.publisher = null;
    this.credentialRejected = false;
    this.connection = 'awaitingAuthentication';
    return {};
  }

  async deliver(params) {
    this.#requireInitialized();
    if (
      !this.connected ||
      !this.socket ||
      !object(params.destination) ||
      typeof params.destination.chatId !== 'string' ||
      params.destination.chatId.trim() === '' ||
      typeof params.idempotencyKey !== 'string' ||
      params.idempotencyKey.trim() === ''
    ) {
      throw new Error('invalid delivery');
    }
    const messageId = outboundMessageId(params.idempotencyKey);
    const content = await outboundContent(params.message, params.attachments);
    const sent = await this.socket.sendMessage(
      params.destination.chatId,
      content,
      { messageId },
    );
    return {
      externalDeliveryId: sent?.key?.id || messageId,
      detail: { sent: true },
    };
  }

  async shutdown() {
    this.shuttingDown = true;
    await this.#stopSocket();
    this.connection = 'stopped';
    return {};
  }

  #prepareAuth(snapshot, committed) {
    this.credentialRejected = false;
    this.auth = createAuthState({ snapshot, ...this.authDependencies });
    this.publisher = new CredentialPublisher({
      bridgeId: this.bridgeId,
      snapshot: () => this.auth.snapshot(),
      callHost: this.callHost,
      onFailure: (error) => this.onFatal(error),
    });
    if (committed) this.publisher.restore(snapshot);
    this.auth.onMutation(() => this.publisher.changed());
  }

  async #connect() {
    if (!this.auth || this.shuttingDown) throw new Error('connection unavailable');
    const generation = ++this.socketGeneration;
    this.connection = 'connecting';
    this.connected = false;
    this.lastQr = null;
    const socket = await this.socketFactory({
      auth: this.auth.state,
      browserName: this.browserName,
    });
    this.socket = socket;
    socket.ev.on('creds.update', () => {
      void this.publisher.changed().catch(() => {});
    });
    socket.ev.on('connection.update', (update) => this.#connectionUpdate(generation, update));
    socket.ev.on('messages.upsert', (event) => this.#messagesUpsert(generation, event));
  }

  #connectionUpdate(generation, update) {
    if (generation !== this.socketGeneration || this.shuttingDown) return;
    if (typeof update.qr === 'string' && update.qr !== '') {
      this.lastQr = update.qr;
      this.connection = 'awaitingAuthentication';
      this.events.emit('qr');
    }
    if (update.connection === 'open') {
      this.connected = true;
      this.connection = 'connected';
      this.credentialRejected = false;
      this.lastQr = null;
      this.reconnectAttempts = 0;
      this.events.emit('connected');
      return;
    }
    if (update.connection !== 'close') return;
    this.connected = false;
    const status = this.disconnectStatus(update.lastDisconnect?.error);
    if (status === this.loggedOutStatus) {
      this.connection = 'loggedOut';
      this.credentialRejected = true;
      return;
    }
    this.connection = 'reconnecting';
    this.reconnectAttempts += 1;
    const waitMs = Math.min(1_000 * (2 ** Math.min(this.reconnectAttempts - 1, 5)), 30_000);
    clearTimeout(this.reconnectTimer);
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      void this.#connect().catch((error) => this.onFatal(error));
    }, waitMs);
    this.reconnectTimer.unref?.();
  }

  #messagesUpsert(generation, event) {
    if (generation !== this.socketGeneration || event.type !== 'notify') return;
    for (const message of event.messages ?? []) {
      if (!inboundAllowed(message, this.inboundPolicy)) continue;
      const inbound = normalizeInbound(message, {
        bridgeId: this.bridgeId,
        targetChannelId: this.targetChannelId,
        now: this.now,
      });
      if (!inbound) continue;
      this.inboundTail = this.inboundTail
        .then(() => this.#persistInbound(inbound, message))
        .catch((error) => this.onFatal(error));
    }
  }

  async #persistInbound(inbound, rawMessage) {
    await this.#storeMedia(inbound, rawMessage);
    await this.#callHostWithRetry('bridge/inbound', inbound);
  }

  async #storeMedia(inbound, rawMessage) {
    const descriptor = mediaDescriptor(rawMessage.message);
    if (!descriptor) return;
    if (descriptor.size !== null && descriptor.size > MAX_MEDIA_BYTES) {
      inbound.message.mediaUnavailable = 'tooLarge';
      return;
    }
    let bytes;
    try {
      bytes = await this.downloadMedia({
        payload: descriptor.payload,
        downloadType: descriptor.downloadType,
        maximumBytes: MAX_MEDIA_BYTES,
      });
    } catch (error) {
      inbound.message.mediaUnavailable = error?.code === 'MEDIA_TOO_LARGE'
        ? 'tooLarge'
        : 'downloadFailed';
      return;
    }
    if (!Buffer.isBuffer(bytes) || bytes.length === 0 || bytes.length > MAX_MEDIA_BYTES) {
      inbound.message.mediaUnavailable = bytes?.length > MAX_MEDIA_BYTES
        ? 'tooLarge'
        : 'downloadFailed';
      return;
    }
    let receipt;
    try {
      receipt = await this.#callHostWithRetry('bridge/content/put', {
        bridgeId: this.bridgeId,
        externalEventId: inbound.externalEventId,
        mediaType: descriptor.mediaType,
        name: descriptor.name,
        bytesBase64: bytes.toString('base64'),
      });
    } catch {
      inbound.message.mediaUnavailable = 'storageFailed';
      return;
    }
    if (
      typeof receipt?.contentHandle !== 'string' ||
      !receipt.contentHandle.startsWith('file://') ||
      receipt.size !== bytes.length ||
      typeof receipt.sha256 !== 'string' ||
      !/^[0-9a-f]{64}$/.test(receipt.sha256)
    ) {
      throw new Error('invalid content acknowledgement');
    }
    inbound.attachments.push({
      mediaType: descriptor.mediaType,
      name: descriptor.name,
      contentHandle: receipt.contentHandle,
    });
  }

  async #callHostWithRetry(method, params) {
    let lastError;
    for (let attempt = 0; attempt < 3; attempt += 1) {
      try {
        return await this.callHost(method, params);
      } catch (error) {
        lastError = error;
        await delay(100 * (2 ** attempt));
      }
    }
    throw lastError;
  }

  async #stopSocket() {
    this.socketGeneration += 1;
    clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
    const socket = this.socket;
    this.socket = null;
    this.connected = false;
    if (socket?.end) {
      try {
        socket.end(new Error('bridge connection reset'));
      } catch {
        // The generation guard already detached this socket.
      }
    }
  }

  #waitFor(event, predicate) {
    const existing = predicate();
    if (existing) return Promise.resolve(existing);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.events.removeListener(event, listener);
        reject(new Error('authentication timed out'));
      }, this.authTimeoutMs);
      timer.unref?.();
      const listener = () => {
        const value = predicate();
        if (!value) return;
        clearTimeout(timer);
        this.events.removeListener(event, listener);
        resolve(value);
      };
      this.events.on(event, listener);
    });
  }

  #requireInitialized() {
    if (!this.initialized || this.shuttingDown) throw new Error('bridge is not active');
  }
}
