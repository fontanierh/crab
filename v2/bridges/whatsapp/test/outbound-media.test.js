import assert from 'node:assert/strict';
import { mkdtemp, rm, symlink, truncate, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { pathToFileURL } from 'node:url';

import { WhatsAppBridgeService } from '../src/bridge-service.js';
import { MAX_MEDIA_BYTES } from '../src/media-policy.js';
import { outboundContent } from '../src/outbound-media.js';
import { credential, dependencies } from './helpers.js';

async function contentFile(t, bytes = Buffer.from('private media')) {
  const directory = await mkdtemp(join(tmpdir(), 'crab-whatsapp-outbound-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const path = join(directory, 'content.blob');
  await writeFile(path, bytes);
  return { path, contentHandle: pathToFileURL(path).href };
}

function attachment(contentHandle, mediaType, name = null) {
  return { contentHandle, mediaType, name };
}

test('maps bounded host content into native WhatsApp media payloads', async (t) => {
  const fixture = await contentFile(t);
  const image = await outboundContent(
    { text: 'caption' },
    [attachment(fixture.contentHandle, 'image/jpeg', 'photo.jpg')],
  );
  assert.equal(image.image.toString(), 'private media');
  assert.equal(image.mimetype, 'image/jpeg');
  assert.equal(image.caption, 'caption');

  const video = await outboundContent(
    { text: 'clip' },
    [attachment(fixture.contentHandle, 'video/mp4', 'clip.mp4')],
  );
  assert.equal(video.video.toString(), 'private media');
  assert.equal(video.caption, 'clip');

  const document = await outboundContent(
    {},
    [attachment(fixture.contentHandle, 'application/pdf', 'brief.pdf')],
  );
  assert.equal(document.document.toString(), 'private media');
  assert.equal(document.fileName, 'brief.pdf');

  const audio = await outboundContent(
    {},
    [attachment(fixture.contentHandle, 'audio/ogg')],
  );
  assert.equal(audio.audio.toString(), 'private media');
  assert.equal(audio.ptt, false);

  const sticker = await outboundContent(
    {},
    [attachment(fixture.contentHandle, 'image/webp')],
  );
  assert.equal(sticker.sticker.toString(), 'private media');
});

test('rejects unsafe, ambiguous, empty, and oversized attachment reads', async (t) => {
  const fixture = await contentFile(t);
  const valid = attachment(fixture.contentHandle, 'image/jpeg');
  await assert.rejects(outboundContent({}, [valid, valid]), /at most one attachment/);
  await assert.rejects(
    outboundContent({}, [attachment('https://example.test/media', 'image/jpeg')]),
    /unavailable/,
  );
  const linkPath = `${fixture.path}.link`;
  await symlink(fixture.path, linkPath);
  await assert.rejects(
    outboundContent({}, [attachment(pathToFileURL(linkPath).href, 'image/jpeg')]),
    /unavailable/,
  );
  await assert.rejects(outboundContent({ text: 'not supported' }, [
    attachment(fixture.contentHandle, 'audio/ogg'),
  ]), /does not support a caption/);

  const empty = await contentFile(t, Buffer.alloc(0));
  await assert.rejects(
    outboundContent({}, [attachment(empty.contentHandle, 'image/jpeg')]),
    /unavailable/,
  );

  const oversized = await contentFile(t);
  await truncate(oversized.path, MAX_MEDIA_BYTES + 1);
  await assert.rejects(
    outboundContent({}, [attachment(oversized.contentHandle, 'image/jpeg')]),
    /exceeds the Crab limit/,
  );
});

test('delivers selected media with a stable WhatsApp message id', async (t) => {
  const fixture = await contentFile(t);
  const { sockets, options } = dependencies({
    callHost: async () => {
      throw new Error('host call is unexpected');
    },
    onFatal: (error) => {
      throw error;
    },
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

  const request = {
    destination: { chatId: 'alice@s.whatsapp.net' },
    message: { text: 'look' },
    attachments: [attachment(fixture.contentHandle, 'image/jpeg', 'photo.jpg')],
    idempotencyKey: 'media-delivery-one',
  };
  const first = await service.deliver(request);
  const second = await service.deliver(request);
  assert.equal(first.externalDeliveryId, second.externalDeliveryId);
  assert.equal(sockets[0].sent[0].options.messageId, sockets[0].sent[1].options.messageId);
  assert.equal(sockets[0].sent[0].content.image.toString(), 'private media');
  assert.equal(sockets[0].sent[0].content.caption, 'look');
});
