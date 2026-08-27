const SKIPPED_TYPES = new Set([
  'protocolMessage',
  'senderKeyDistributionMessage',
  'messageContextInfo',
  'deviceSentMessage',
  'bcallMessage',
  'callLogMesssage',
  'editedMessage',
]);

function unwrapMessage(message) {
  let current = message;
  for (let depth = 0; depth < 4 && current; depth += 1) {
    const wrapped =
      current.ephemeralMessage?.message ??
      current.viewOnceMessage?.message ??
      current.viewOnceMessageV2?.message ??
      current.documentWithCaptionMessage?.message;
    if (!wrapped) break;
    current = wrapped;
  }
  return current;
}

function contentType(message) {
  return Object.keys(message ?? {}).find((key) => message[key] !== null && message[key] !== undefined);
}

function quotedText(context) {
  const quoted = unwrapMessage(context?.quotedMessage);
  const type = contentType(quoted);
  if (type === 'conversation') return quoted.conversation ?? null;
  if (type === 'extendedTextMessage') return quoted.extendedTextMessage?.text ?? null;
  if (type === 'imageMessage') return quoted.imageMessage?.caption ?? '[image]';
  if (type === 'videoMessage') return quoted.videoMessage?.caption ?? '[video]';
  return type ? `[${type.replace(/Message$/, '')}]` : null;
}

function extractContent(message) {
  const inner = unwrapMessage(message);
  const type = contentType(inner);
  if (!type || SKIPPED_TYPES.has(type)) return null;
  const payload = inner[type];
  const result = {
    type: type.replace(/Message$/, ''),
    text: null,
    caption: null,
    mimeType: null,
    fileName: null,
    quotedText: null,
    mentions: [],
  };

  switch (type) {
    case 'conversation':
      result.type = 'text';
      result.text = inner.conversation || '';
      break;
    case 'extendedTextMessage':
      result.type = 'text';
      result.text = payload?.text || '';
      result.quotedText = quotedText(payload?.contextInfo);
      result.mentions = payload?.contextInfo?.mentionedJid || [];
      break;
    case 'imageMessage':
    case 'videoMessage':
      result.caption = payload?.caption || null;
      result.mimeType = payload?.mimetype || null;
      result.quotedText = quotedText(payload?.contextInfo);
      break;
    case 'audioMessage':
      result.type = payload?.ptt ? 'voiceNote' : 'audio';
      result.mimeType = payload?.mimetype || null;
      break;
    case 'documentMessage':
      result.caption = payload?.caption || null;
      result.mimeType = payload?.mimetype || null;
      result.fileName = payload?.fileName || null;
      result.quotedText = quotedText(payload?.contextInfo);
      break;
    case 'stickerMessage':
      result.mimeType = payload?.mimetype || null;
      break;
    case 'reactionMessage':
      result.text = payload?.text || '';
      break;
    case 'locationMessage':
      result.location = {
        latitude: payload?.degreesLatitude ?? null,
        longitude: payload?.degreesLongitude ?? null,
        name: payload?.name ?? null,
      };
      break;
    case 'contactMessage':
      result.text = payload?.displayName || '';
      break;
    default:
      return null;
  }
  return result;
}

const MEDIA_TYPES = {
  imageMessage: { downloadType: 'image', defaultMediaType: 'image/jpeg' },
  videoMessage: { downloadType: 'video', defaultMediaType: 'video/mp4' },
  audioMessage: { downloadType: 'audio', defaultMediaType: 'audio/ogg' },
  documentMessage: { downloadType: 'document', defaultMediaType: 'application/octet-stream' },
  stickerMessage: { downloadType: 'sticker', defaultMediaType: 'image/webp' },
};

export function mediaDescriptor(message) {
  const inner = unwrapMessage(message);
  const type = contentType(inner);
  const media = MEDIA_TYPES[type];
  const payload = inner?.[type];
  if (!media || !payload || typeof payload !== 'object') return null;
  const rawSize = Number(payload.fileLength);
  return {
    payload,
    downloadType: media.downloadType,
    mediaType: payload.mimetype || media.defaultMediaType,
    name: type === 'documentMessage' ? payload.fileName || null : null,
    size: Number.isSafeInteger(rawSize) && rawSize >= 0 ? rawSize : null,
  };
}

export function normalizeInbound(message, { bridgeId, targetChannelId, now = Date.now }) {
  const remoteJid = message?.key?.remoteJid;
  const messageId = message?.key?.id;
  if (
    message?.key?.fromMe ||
    !remoteJid ||
    !messageId ||
    remoteJid === 'status@broadcast' ||
    !message.message
  ) {
    return null;
  }
  const content = extractContent(message.message);
  if (!content) return null;
  const isGroup = remoteJid.endsWith('@g.us');
  const senderJid = (isGroup ? message.key.participant : remoteJid) || remoteJid;
  const timestampSeconds = Number(message.messageTimestamp);
  const receivedAtMs = Number.isFinite(timestampSeconds) && timestampSeconds > 0
    ? Math.trunc(timestampSeconds * 1000)
    : now();

  return {
    bridgeId,
    externalEventId: `${remoteJid}:${messageId}`,
    receivedAtMs,
    targetChannelId,
    sender: {
      externalUserId: senderJid,
      displayName: message.pushName || null,
      chatId: remoteJid,
      isGroup,
    },
    message: {
      ...content,
      chatId: remoteJid,
    },
    attachments: [],
  };
}
