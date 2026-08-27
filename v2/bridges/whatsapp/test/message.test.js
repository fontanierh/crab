import assert from 'node:assert/strict';
import test from 'node:test';

import { mediaDescriptor, normalizeInbound } from '../src/message.js';

const options = { bridgeId: 'whatsapp', targetChannelId: 'primary', now: () => 99 };

test('normalizes inbound text with stable deduplication and sender metadata', () => {
  const inbound = normalizeInbound({
    key: { id: 'message-1', remoteJid: 'group@g.us', participant: 'alice@s.whatsapp.net' },
    pushName: 'Alice',
    messageTimestamp: 123,
    message: { extendedTextMessage: { text: 'hello', contextInfo: {} } },
  }, options);
  assert.equal(inbound.externalEventId, 'group@g.us:message-1');
  assert.equal(inbound.receivedAtMs, 123_000);
  assert.deepEqual(inbound.sender, {
    externalUserId: 'alice@s.whatsapp.net',
    displayName: 'Alice',
    chatId: 'group@g.us',
    isGroup: true,
  });
  assert.equal(inbound.message.text, 'hello');
  assert.deepEqual(inbound.attachments, []);
});

test('emits media metadata and a private download descriptor', () => {
  const message = {
    key: { id: 'image-1', remoteJid: 'alice@s.whatsapp.net' },
    message: {
      imageMessage: {
        caption: 'diagram',
        mimetype: 'image/jpeg',
        fileLength: 42,
        mediaKey: Buffer.from('private'),
      },
    },
  };
  const inbound = normalizeInbound(message, options);
  assert.equal(inbound.message.type, 'image');
  assert.equal(inbound.message.caption, 'diagram');
  assert.equal(inbound.message.mimeType, 'image/jpeg');
  assert.deepEqual(inbound.attachments, []);
  assert.deepEqual(mediaDescriptor(message.message), {
    payload: message.message.imageMessage,
    downloadType: 'image',
    mediaType: 'image/jpeg',
    name: null,
    size: 42,
  });
});

test('drops own, status, and protocol traffic', () => {
  assert.equal(normalizeInbound({
    key: { id: 'own', remoteJid: 'alice@s.whatsapp.net', fromMe: true },
    message: { conversation: 'ignore' },
  }, options), null);
  assert.equal(normalizeInbound({
    key: { id: 'status', remoteJid: 'status@broadcast' },
    message: { conversation: 'ignore' },
  }, options), null);
  assert.equal(normalizeInbound({
    key: { id: 'protocol', remoteJid: 'alice@s.whatsapp.net' },
    message: { protocolMessage: {} },
  }, options), null);
});
