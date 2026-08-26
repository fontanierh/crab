#!/usr/bin/env node

import { baileysDependencies } from './baileys-adapter.js';
import { WhatsAppBridgeService } from './bridge-service.js';
import { ProtocolPeer } from './protocol.js';

let terminating = false;
let peer;

function fatal() {
  if (terminating) return;
  terminating = true;
  process.stderr.write('whatsapp bridge stopped after a non-recoverable runtime error\n');
  peer?.close();
  process.exitCode = 1;
  setImmediate(() => process.exit(1));
}

const service = new WhatsAppBridgeService({
  ...baileysDependencies,
  callHost: (method, params) => peer.callHost(method, params),
  onFatal: fatal,
});

peer = new ProtocolPeer({
  input: process.stdin,
  output: process.stdout,
  service,
});
peer.start();

async function shutdown() {
  if (terminating) return;
  terminating = true;
  try {
    await service.shutdown();
  } finally {
    peer.close();
    process.exit(0);
  }
}

process.on('SIGINT', () => void shutdown());
process.on('SIGTERM', () => void shutdown());
process.on('uncaughtException', fatal);
process.on('unhandledRejection', fatal);
