export const MAX_MEDIA_BYTES = 8 * 1024 * 1024;

export async function collectMedia(stream, maximumBytes = MAX_MEDIA_BYTES) {
  const chunks = [];
  let size = 0;
  for await (const chunk of stream) {
    const bytes = Buffer.from(chunk);
    size += bytes.length;
    if (size > maximumBytes) {
      stream.destroy?.();
      const error = new Error('media exceeds the Crab content limit');
      error.code = 'MEDIA_TOO_LARGE';
      throw error;
    }
    chunks.push(bytes);
  }
  return Buffer.concat(chunks, size);
}
