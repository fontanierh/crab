import makeWASocket, {
  Browsers,
  BufferJSON,
  DisconnectReason,
  downloadContentFromMessage,
  initAuthCreds,
  proto,
} from '@whiskeysockets/baileys';

import { MAX_MEDIA_BYTES, collectMedia } from './media-policy.js';

async function downloadMedia({ payload, downloadType, maximumBytes = MAX_MEDIA_BYTES }) {
  const stream = await downloadContentFromMessage(payload, downloadType);
  return collectMedia(stream, maximumBytes);
}

const silentLogger = {
  level: 'silent',
  trace() {},
  debug() {},
  info() {},
  warn() {},
  error() {},
  fatal() {},
  child() { return this; },
};

export const baileysDependencies = {
  initAuthCreds,
  bufferJson: BufferJSON,
  appStateSyncKeyFromObject: (value) => proto.Message.AppStateSyncKeyData.fromObject(value),
  loggedOutStatus: DisconnectReason.loggedOut,
  disconnectStatus: (error) => error?.output?.statusCode ?? error?.data?.statusCode ?? null,
  downloadMedia,
  socketFactory: async ({ auth, browserName }) => makeWASocket({
    auth,
    browser: Browsers.macOS(browserName),
    emitOwnEvents: false,
    generateHighQualityLinkPreview: false,
    logger: silentLogger,
    markOnlineOnConnect: false,
    printQRInTerminal: false,
    syncFullHistory: false,
  }),
};
