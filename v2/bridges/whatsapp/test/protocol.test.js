import assert from 'node:assert/strict';
import { PassThrough } from 'node:stream';
import test from 'node:test';

import { ProtocolPeer } from '../src/protocol.js';

function nextLine(stream) {
  return new Promise((resolve) => {
    stream.once('data', (chunk) => resolve(JSON.parse(String(chunk).trim())));
  });
}

test('protocol returns strict v2 initialization and redacts service failures', async () => {
  const input = new PassThrough();
  const output = new PassThrough();
  const service = {
    async initialize() { return { protocolVersion: 2 }; },
    async health() { throw new Error('credential=super-secret'); },
  };
  const peer = new ProtocolPeer({ input, output, service });
  peer.start();

  let response = nextLine(output);
  input.write(`${JSON.stringify({ jsonrpc: '2.0', id: 'one', method: 'bridge/initialize', params: {} })}\n`);
  assert.deepEqual(await response, { jsonrpc: '2.0', id: 'one', result: { protocolVersion: 2 } });

  response = nextLine(output);
  input.write(`${JSON.stringify({ jsonrpc: '2.0', id: 'two', method: 'bridge/health', params: {} })}\n`);
  const rejected = await response;
  assert.equal(rejected.error.code, 'PackageRejected');
  assert.equal(JSON.stringify(rejected).includes('super-secret'), false);
  peer.close();
});

test('protocol multiplexes package-to-host calls', async () => {
  const input = new PassThrough();
  const output = new PassThrough();
  const peer = new ProtocolPeer({ input, output, service: {} });
  peer.start();
  const requestLine = nextLine(output);
  const result = peer.callHost('bridge/inbound', { externalEventId: 'one' });
  const request = await requestLine;
  assert.equal(request.method, 'bridge/inbound');
  input.write(`${JSON.stringify({ jsonrpc: '2.0', id: request.id, result: { triggerId: 'trigger-1' } })}\n`);
  assert.deepEqual(await result, { triggerId: 'trigger-1' });
  peer.close();
});
