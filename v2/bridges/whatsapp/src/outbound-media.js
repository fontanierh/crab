import { lstat, open } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';

import { MAX_MEDIA_BYTES } from './media-policy.js';

function object(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function caption(message) {
  if (!object(message) || (message.text !== undefined && typeof message.text !== 'string')) {
    throw new Error('invalid delivery message');
  }
  return message.text?.trim() ? message.text : null;
}

function validateAttachment(attachment) {
  if (
    !object(attachment) ||
    typeof attachment.mediaType !== 'string' ||
    attachment.mediaType.trim() === '' ||
    attachment.mediaType.length > 255 ||
    typeof attachment.contentHandle !== 'string' ||
    attachment.contentHandle.length > 4096 ||
    (attachment.name !== null &&
      attachment.name !== undefined &&
      (typeof attachment.name !== 'string' ||
        attachment.name.trim() === '' ||
        attachment.name.length > 255))
  ) {
    throw new Error('invalid delivery attachment');
  }
}

async function readBoundedFile(contentHandle) {
  let path;
  try {
    const url = new URL(contentHandle);
    if (url.protocol !== 'file:') throw new Error('not a file handle');
    path = fileURLToPath(url);
    const pathMetadata = await lstat(path);
    if (!pathMetadata.isFile() || pathMetadata.isSymbolicLink()) {
      throw new Error('not a regular file');
    }
  } catch {
    throw new Error('attachment content is unavailable');
  }

  let file;
  try {
    file = await open(path, 'r');
    const metadata = await file.stat();
    if (!metadata.isFile() || metadata.size === 0) {
      throw new Error('attachment content is unavailable');
    }
    if (metadata.size > MAX_MEDIA_BYTES) {
      throw new Error('attachment content exceeds the Crab limit');
    }
    const bytes = Buffer.alloc(metadata.size + 1);
    const { bytesRead } = await file.read(bytes, 0, bytes.length, 0);
    if (bytesRead === 0 || bytesRead > MAX_MEDIA_BYTES || bytesRead !== metadata.size) {
      throw new Error('attachment content changed while reading');
    }
    return bytes.subarray(0, bytesRead);
  } finally {
    await file?.close();
  }
}

function mediaPayload(message, attachment, bytes) {
  const text = caption(message);
  const mediaType = attachment.mediaType.trim().toLowerCase();
  if (mediaType === 'image/webp' && text === null) {
    return { sticker: bytes, mimetype: mediaType };
  }
  if (mediaType.startsWith('image/')) {
    return { image: bytes, mimetype: mediaType, ...(text === null ? {} : { caption: text }) };
  }
  if (mediaType.startsWith('video/')) {
    return { video: bytes, mimetype: mediaType, ...(text === null ? {} : { caption: text }) };
  }
  if (mediaType.startsWith('audio/')) {
    if (text !== null) throw new Error('audio delivery does not support a caption');
    return { audio: bytes, mimetype: mediaType, ptt: false };
  }
  return {
    document: bytes,
    mimetype: mediaType,
    fileName: attachment.name ?? 'attachment',
    ...(text === null ? {} : { caption: text }),
  };
}

export async function outboundContent(message, attachments) {
  if (!Array.isArray(attachments) || attachments.length > 1) {
    throw new Error('WhatsApp delivery accepts at most one attachment');
  }
  if (attachments.length === 0) {
    const text = caption(message);
    if (text === null) throw new Error('text delivery is empty');
    return { text };
  }
  const attachment = attachments[0];
  validateAttachment(attachment);
  const bytes = await readBoundedFile(attachment.contentHandle);
  return mediaPayload(message, attachment, bytes);
}
