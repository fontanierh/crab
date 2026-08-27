import assert from 'node:assert/strict';
import test from 'node:test';

import { collectMedia } from '../src/media-policy.js';

test('collects a bounded media stream without package-local files', async () => {
  async function* chunks() {
    yield Buffer.from('first');
    yield Buffer.from(' second');
  }
  assert.equal((await collectMedia(chunks(), 12)).toString(), 'first second');
});

test('stops a stream before accepting bytes beyond the host limit', async () => {
  let destroyed = false;
  const stream = {
    async *[Symbol.asyncIterator]() {
      yield Buffer.from('1234');
      yield Buffer.from('5');
    },
    destroy() { destroyed = true; },
  };
  await assert.rejects(collectMedia(stream, 4), { code: 'MEDIA_TOO_LARGE' });
  assert.equal(destroyed, true);
});
