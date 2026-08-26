import makeWASocket, {
  Browsers,
  BufferJSON,
  DisconnectReason,
  initAuthCreds,
  proto,
} from '@whiskeysockets/baileys';

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
