import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import test from 'node:test';

import {
  CredentialPublisher,
  InboundPublisher,
  WhatsAppBridgeService,
} from '../src/bridge-service.js';
import { MAX_CREDENTIAL_SNAPSHOT_BYTES } from '../src/auth-state.js';
import { credentialFingerprint } from '../src/canonical-json.js';
import { credential, dependencies, flush } from './helpers.js';

async function initialize(service) {
  return service.initialize({
    protocolVersion: 2,
    bridgeId: 'whatsapp',
    packageId: 'first-party-whatsapp',
    configuration: {
      targetChannelId: 'primary',
      browserName: 'Crab',
      inboundPolicy: {
        directChatIds: ['alice@s.whatsapp.net'],
        groups: [{ chatId: 'group@g.us', senderIds: ['alice@s.whatsapp.net'] }],
      },
    },
  });
}

test('credential mutation bursts coalesce behind one durable CAS', async () => {
  const committed = credential();
  let current = structuredClone(committed);
  let releaseFirst;
  const firstCall = new Promise((resolve) => { releaseFirst = resolve; });
  const updates = [];
  const publisher = new CredentialPublisher({
    bridgeId: 'whatsapp',
    snapshot: () => structuredClone(current),
    callHost: async (method, params) => {
      assert.equal(method, 'bridge/credential/update');
      updates.push(params);
      if (updates.length === 1) await firstCall;
      return { credentialFingerprint: credentialFingerprint(params.credential) };
    },
    onFailure: (error) => { throw error; },
  });
  publisher.restore(committed);

  current.keys.session = { alice: { revision: 1 } };
  const first = publisher.changed();
  assert.equal(updates.length, 1);
  const waiters = [first];
  for (let revision = 2; revision <= 1_000; revision += 1) {
    current.keys.session.alice = { revision };
    waiters.push(publisher.changed());
  }
  assert.equal(updates.length, 1, 'only one CAS is in flight');
  assert.ok(waiters.every((waiter) => waiter === first));

  releaseFirst();
  await Promise.all(waiters);
  assert.equal(updates.length, 2, 'the burst becomes one newest-snapshot follow-up');
  assert.equal(updates[0].previousFingerprint, credentialFingerprint(committed));
  assert.equal(
    updates[1].previousFingerprint,
    credentialFingerprint(updates[0].credential),
  );
  assert.equal(updates[1].credential.keys.session.alice.revision, 1_000);
});

test('credential persistence failure is terminal for the package instance', async () => {
  const committed = credential();
  const current = structuredClone(committed);
  current.keys.session = { alice: { revision: 1 } };
  const failure = new Error('host CAS unavailable');
  const failures = [];
  let attempts = 0;
  const publisher = new CredentialPublisher({
    bridgeId: 'whatsapp',
    snapshot: () => structuredClone(current),
    callHost: async () => {
      attempts += 1;
      throw failure;
    },
    onFailure: (error) => failures.push(error),
  });
  publisher.restore(committed);

  await assert.rejects(publisher.changed(), failure);
  current.keys.session.alice = { revision: 2 };
  await assert.rejects(publisher.changed(), failure);
  assert.equal(attempts, 1, 'a stale fingerprint is never retried in-process');
  assert.deepEqual(failures, [failure]);
});

test('oversized live credential growth fails before calling the host', async () => {
  const failures = [];
  let hostCalls = 0;
  const { sockets, options } = dependencies({
    callHost: async () => {
      hostCalls += 1;
      throw new Error('must not publish an oversized credential');
    },
    onFatal: (error) => failures.push(error),
  });
  const service = new WhatsAppBridgeService(options);
  await initialize(service);
  await service.health({ credential: credential() });

  await assert.rejects(sockets[0].auth.keys.set({
    session: { oversized: 'x'.repeat(MAX_CREDENTIAL_SNAPSHOT_BYTES) },
  }), /snapshot too large/);
  assert.equal(hostCalls, 0);
  assert.equal(failures.length, 1);
  assert.match(failures[0].message, /snapshot too large/);
});

test('inbound bursts share one bounded FIFO persistence drain', async () => {
  let releaseFirst;
  const firstCall = new Promise((resolve) => { releaseFirst = resolve; });
  const persisted = [];
  const publisher = new InboundPublisher({
    capacity: 3,
    persist: async (inbound, rawMessage) => {
      persisted.push([inbound.externalEventId, rawMessage.key.id]);
      if (persisted.length === 1) await firstCall;
    },
    onFailure: (error) => { throw error; },
  });
  const item = (id) => ({
    inbound: { externalEventId: id },
    rawMessage: { key: { id } },
  });

  const first = publisher.enqueue([item('one')]);
  assert.equal(publisher.remainingCapacity(), 2);
  const queued = publisher.enqueue([item('two'), item('three')]);
  assert.equal(queued, first, 'all admitted messages share one drain promise');
  assert.equal(publisher.remainingCapacity(), 0);
  assert.deepEqual(persisted, [['one', 'one']], 'only one persistence call is active');

  releaseFirst();
  await first;
  assert.deepEqual(persisted, [
    ['one', 'one'],
    ['two', 'two'],
    ['three', 'three'],
  ]);
});

test('inbound backlog overflow fails the package instance once and drops queued work', async () => {
  let releaseFirst;
  const firstCall = new Promise((resolve) => { releaseFirst = resolve; });
  const persisted = [];
  const failures = [];
  const publisher = new InboundPublisher({
    capacity: 2,
    persist: async (inbound) => {
      persisted.push(inbound.externalEventId);
      if (persisted.length === 1) await firstCall;
    },
    onFailure: (error) => failures.push(error),
  });
  const item = (id) => ({ inbound: { externalEventId: id }, rawMessage: {} });

  const running = publisher.enqueue([item('one')]);
  assert.equal(publisher.enqueue([item('two')]), running);
  assert.equal(publisher.enqueue([item('overflow')]), running);
  assert.equal(failures.length, 1);
  assert.match(failures[0].message, /backlog exceeded/);

  const rejected = assert.rejects(running, failures[0]);
  releaseFirst();
  await rejected;
  await assert.rejects(publisher.enqueue([item('later')]), failures[0]);
  assert.deepEqual(persisted, ['one']);
  assert.equal(failures.length, 1, 'supervision receives one terminal failure');
});

test('restores a committed snapshot and publishes ordered key rotations', async () => {
  const updates = [];
  const { sockets, options } = dependencies({
    callHost: async (method, params) => {
      assert.equal(method, 'bridge/credential/update');
      updates.push(params);
      return { credentialFingerprint: credentialFingerprint(params.credential) };
    },
    onFatal: (error) => { throw error; },
  });
  const service = new WhatsAppBridgeService(options);
  await initialize(service);
  const original = credential();
  const starting = await service.health({ credential: original });
  assert.equal(starting.credentialValid, true);
  assert.equal(sockets.length, 1);
  sockets[0].ev.emit('connection.update', { connection: 'open' });
  await sockets[0].auth.keys.set({ session: { alice: { revision: 1 } } });
  await sockets[0].auth.keys.set({ session: { alice: { revision: 2 } } });
  assert.equal(updates.length, 2);
  assert.equal(updates[0].previousFingerprint, credentialFingerprint(original));
  assert.equal(
    updates[1].previousFingerprint,
    credentialFingerprint(updates[0].credential),
  );

  const restarted = dependencies({
    callHost: async () => { throw new Error('unexpected update'); },
    onFatal: (error) => { throw error; },
  });
  const restartedService = new WhatsAppBridgeService(restarted.options);
  await initialize(restartedService);
  const restored = await restartedService.health({ credential: updates[1].credential });
  assert.equal(restored.credentialValid, true);
  const restoredKey = await restarted.sockets[0].auth.keys.get('session', ['alice']);
  assert.deepEqual(restoredKey.alice, { revision: 2 });
});

test('QR pairing buffers mutations until Crab acknowledges the initial snapshot', async () => {
  const calls = [];
  const { sockets, options } = dependencies({
    callHost: async (method, params) => {
      calls.push({ method, params });
      return { credentialFingerprint: credentialFingerprint(params.credential) };
    },
    onFatal: (error) => { throw error; },
  });
  const service = new WhatsAppBridgeService(options);
  await initialize(service);
  const challengePromise = service.beginAuthentication({ method: 'qrCode', context: {} });
  await flush();
  sockets[0].ev.emit('connection.update', { qr: 'qr-payload' });
  const challenge = await challengePromise;
  assert.deepEqual(challenge.presentation, { kind: 'qrCode', value: 'qr-payload' });

  sockets[0].auth.creds.registered = true;
  sockets[0].ev.emit('creds.update', {});
  sockets[0].ev.emit('connection.update', { connection: 'open' });
  const submitted = await service.submitAuthentication({ challengeId: 'one', response: {} });
  await sockets[0].auth.keys.set({ session: { beforeCommit: { revision: 1 } } });
  assert.equal(calls.length, 0);

  await service.credentialCommitted({ credential: submitted.credential });
  assert.equal(calls.length, 1, 'post-submit key changes flush after the commit barrier');
  await sockets[0].auth.keys.set({ session: { afterCommit: { revision: 2 } } });
  assert.equal(calls.length, 2);
});

test('phone pairing, inbound routing, and deterministic text delivery use no local state', async () => {
  const calls = [];
  const { sockets, options } = dependencies({
    callHost: async (method, params) => {
      calls.push({ method, params });
      return method === 'bridge/inbound'
        ? { triggerId: 'trigger-1' }
        : { credentialFingerprint: credentialFingerprint(params.credential) };
    },
    onFatal: (error) => { throw error; },
  });
  const service = new WhatsAppBridgeService(options);
  await initialize(service);
  const challenge = await service.beginAuthentication({
    method: 'phoneCode',
    context: { phoneNumber: '+33 6 00 00 00 00' },
  });
  assert.deepEqual(challenge.presentation, { kind: 'phoneCode', code: '1234-5678' });
  sockets[0].auth.creds.registered = true;
  sockets[0].ev.emit('connection.update', { connection: 'open' });

  sockets[0].ev.emit('messages.upsert', {
    type: 'notify',
    messages: [{
      key: { id: 'incoming-1', remoteJid: 'alice@s.whatsapp.net' },
      pushName: 'Alice',
      messageTimestamp: 5,
      message: { conversation: 'hello Crab' },
    }],
  });
  await flush();
  const inbound = calls.find((call) => call.method === 'bridge/inbound');
  assert.equal(inbound.params.message.text, 'hello Crab');

  const first = await service.deliver({
    destination: { chatId: 'alice@s.whatsapp.net' },
    message: { text: 'hello Alice' },
    attachments: [],
    idempotencyKey: 'delivery-one',
  });
  const second = await service.deliver({
    destination: { chatId: 'alice@s.whatsapp.net' },
    message: { text: 'hello Alice' },
    attachments: [],
    idempotencyKey: 'delivery-one',
  });
  assert.equal(first.externalDeliveryId, second.externalDeliveryId);
  assert.equal(sockets[0].sent[0].options.messageId, sockets[0].sent[1].options.messageId);
});

test('inbound media is durably stored before its trigger references the host handle', async () => {
  const calls = [];
  const bytes = Buffer.from('private image bytes');
  const { sockets, options } = dependencies({
    downloadMedia: async ({ downloadType, maximumBytes }) => {
      assert.equal(downloadType, 'image');
      assert.equal(maximumBytes, 8 * 1024 * 1024);
      return bytes;
    },
    callHost: async (method, params) => {
      calls.push({ method, params });
      if (method === 'bridge/content/put') {
        return {
          contentHandle: 'file:///private/crab/content_1.blob',
          size: bytes.length,
          sha256: createHash('sha256').update(bytes).digest('hex'),
        };
      }
      return { triggerId: 'trigger-media-1' };
    },
    onFatal: (error) => { throw error; },
  });
  const service = new WhatsAppBridgeService(options);
  await initialize(service);
  await service.health({ credential: credential() });
  sockets[0].ev.emit('connection.update', { connection: 'open' });
  sockets[0].ev.emit('messages.upsert', {
    type: 'notify',
    messages: [{
      key: { id: 'image-1', remoteJid: 'alice@s.whatsapp.net' },
      message: {
        imageMessage: {
          caption: 'diagram',
          mimetype: 'image/jpeg',
          fileLength: bytes.length,
          mediaKey: Buffer.from('not-forwarded'),
        },
      },
    }],
  });
  await flush();
  assert.deepEqual(calls.map((call) => call.method), [
    'bridge/content/put',
    'bridge/inbound',
  ]);
  assert.equal(calls[0].params.bytesBase64, bytes.toString('base64'));
  assert.deepEqual(calls[1].params.attachments, [{
    mediaType: 'image/jpeg',
    name: null,
    contentHandle: 'file:///private/crab/content_1.blob',
  }]);
  assert.equal(calls[1].params.message.mediaKey, undefined);
});

test('oversized media keeps truthful metadata without downloading bytes', async () => {
  const calls = [];
  const { sockets, options } = dependencies({
    downloadMedia: async () => { throw new Error('must not download oversized media'); },
    callHost: async (method, params) => {
      calls.push({ method, params });
      return { triggerId: 'trigger-large-1' };
    },
    onFatal: (error) => { throw error; },
  });
  const service = new WhatsAppBridgeService(options);
  await initialize(service);
  await service.health({ credential: credential() });
  sockets[0].ev.emit('connection.update', { connection: 'open' });
  sockets[0].ev.emit('messages.upsert', {
    type: 'notify',
    messages: [{
      key: { id: 'video-1', remoteJid: 'alice@s.whatsapp.net' },
      message: { videoMessage: { mimetype: 'video/mp4', fileLength: 8 * 1024 * 1024 + 1 } },
    }],
  });
  await flush();
  assert.deepEqual(calls.map((call) => call.method), ['bridge/inbound']);
  assert.equal(calls[0].params.message.mediaUnavailable, 'tooLarge');
  assert.deepEqual(calls[0].params.attachments, []);
});

test('a failed media download preserves the inbound message and bridge process', async () => {
  const calls = [];
  const { sockets, options } = dependencies({
    downloadMedia: async () => { throw new Error('network unavailable'); },
    callHost: async (method, params) => {
      calls.push({ method, params });
      return { triggerId: 'trigger-download-failed' };
    },
    onFatal: (error) => { throw error; },
  });
  const service = new WhatsAppBridgeService(options);
  await initialize(service);
  await service.health({ credential: credential() });
  sockets[0].ev.emit('connection.update', { connection: 'open' });
  sockets[0].ev.emit('messages.upsert', {
    type: 'notify',
    messages: [{
      key: { id: 'audio-1', remoteJid: 'alice@s.whatsapp.net' },
      message: { audioMessage: { mimetype: 'audio/ogg', fileLength: 20 } },
    }],
  });
  await flush();
  assert.deepEqual(calls.map((call) => call.method), ['bridge/inbound']);
  assert.equal(calls[0].params.message.mediaUnavailable, 'downloadFailed');
});

test('unauthorized traffic is dropped before media download or any host call', async () => {
  const calls = [];
  let downloads = 0;
  const { sockets, options } = dependencies({
    downloadMedia: async () => {
      downloads += 1;
      return Buffer.from('must not be read');
    },
    callHost: async (method, params) => {
      calls.push({ method, params });
      return { triggerId: 'unexpected' };
    },
    onFatal: (error) => { throw error; },
  });
  const service = new WhatsAppBridgeService(options);
  await service.initialize({
    protocolVersion: 2,
    bridgeId: 'whatsapp',
    packageId: 'first-party-whatsapp',
    configuration: { targetChannelId: 'primary' },
  });
  await service.health({ credential: credential() });
  sockets[0].ev.emit('connection.update', { connection: 'open' });
  sockets[0].ev.emit('messages.upsert', {
    type: 'notify',
    messages: [{
      key: { id: 'blocked-1', remoteJid: 'alice@s.whatsapp.net' },
      message: { imageMessage: { mimetype: 'image/jpeg', fileLength: 10 } },
    }, {
      key: {
        id: 'blocked-2',
        remoteJid: 'group@g.us',
        participant: 'alice@s.whatsapp.net',
      },
      message: { conversation: 'ignore this' },
    }],
  });
  await flush();
  assert.equal(downloads, 0);
  assert.deepEqual(calls, []);
});

test('initialization rejects malformed inbound policy instead of starting permissively', async () => {
  const { options } = dependencies({
    callHost: async () => { throw new Error('unexpected host call'); },
    onFatal: (error) => { throw error; },
  });
  const service = new WhatsAppBridgeService(options);
  await assert.rejects(service.initialize({
    protocolVersion: 2,
    bridgeId: 'whatsapp',
    packageId: 'first-party-whatsapp',
    configuration: {
      targetChannelId: 'primary',
      inboundPolicy: { groups: [{ chatId: 'team@g.us', senderIds: [] }] },
    },
  }), /group policy requires a sender/);
});

test('a server-side logout makes credential health fail closed', async () => {
  const { sockets, options } = dependencies({
    callHost: async () => { throw new Error('unexpected host call'); },
    onFatal: (error) => { throw error; },
  });
  const service = new WhatsAppBridgeService(options);
  await initialize(service);
  const snapshot = credential();
  await service.health({ credential: snapshot });
  sockets[0].ev.emit('connection.update', {
    connection: 'close',
    lastDisconnect: { error: { status: 401 } },
  });
  const health = await service.health({ credential: snapshot });
  const validation = await service.validateCredentials({ credential: snapshot });
  assert.equal(health.credentialValid, false);
  assert.equal(health.detail.connection, 'loggedOut');
  assert.equal(validation.valid, false);
});
