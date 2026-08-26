import assert from 'node:assert/strict';
import test from 'node:test';

import { WhatsAppBridgeService } from '../src/bridge-service.js';
import { credentialFingerprint } from '../src/canonical-json.js';
import { credential, dependencies, flush } from './helpers.js';

async function initialize(service) {
  return service.initialize({
    protocolVersion: 2,
    bridgeId: 'whatsapp',
    packageId: 'first-party-whatsapp',
    configuration: { targetChannelId: 'primary', browserName: 'Crab' },
  });
}

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
