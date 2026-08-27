const MAX_POLICY_IDENTITIES = 1_024;

function object(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function exactIdentifier(value) {
  return typeof value === 'string' && value !== '' && value.trim() === value;
}

function identifiers(value, field) {
  if (value === undefined) return [];
  if (!Array.isArray(value) || value.some((entry) => !exactIdentifier(entry))) {
    throw new Error(`invalid ${field}`);
  }
  const unique = new Set(value);
  if (unique.size !== value.length) throw new Error(`duplicate ${field}`);
  return value;
}

export function parseInboundPolicy(value) {
  if (value === undefined) {
    return { directChatIds: new Set(), groups: new Map() };
  }
  if (!object(value)) throw new Error('invalid inbound policy');
  const allowed = new Set(['directChatIds', 'groups']);
  if (Object.keys(value).some((key) => !allowed.has(key))) {
    throw new Error('unknown inbound policy field');
  }

  const directChatIds = identifiers(value.directChatIds, 'direct chat id');
  if (directChatIds.some((chatId) => chatId.endsWith('@g.us'))) {
    throw new Error('group chat requires an exact sender rule');
  }
  if (value.groups !== undefined && !Array.isArray(value.groups)) {
    throw new Error('invalid group policy');
  }
  const groups = new Map();
  let identityCount = directChatIds.length;
  for (const group of value.groups ?? []) {
    if (
      !object(group) ||
      Object.keys(group).some((key) => key !== 'chatId' && key !== 'senderIds') ||
      !exactIdentifier(group.chatId) ||
      !group.chatId.endsWith('@g.us') ||
      groups.has(group.chatId)
    ) {
      throw new Error('invalid group policy');
    }
    const senderIds = identifiers(group.senderIds, 'group sender id');
    if (senderIds.length === 0) throw new Error('group policy requires a sender');
    groups.set(group.chatId, new Set(senderIds));
    identityCount += 1 + senderIds.length;
  }
  if (identityCount > MAX_POLICY_IDENTITIES) {
    throw new Error('inbound policy is too large');
  }
  return { directChatIds: new Set(directChatIds), groups };
}

function candidates(...values) {
  return values.filter((value) => exactIdentifier(value));
}

export function inboundAllowed(message, policy) {
  if (message?.key?.fromMe) return false;
  const remoteJid = message?.key?.remoteJid;
  if (!exactIdentifier(remoteJid)) return false;
  if (remoteJid.endsWith('@g.us')) {
    const allowedSenders = policy.groups.get(remoteJid);
    return Boolean(allowedSenders) && candidates(
      message.key.participant,
      message.key.participantAlt,
    ).some((senderId) => allowedSenders.has(senderId));
  }
  return candidates(remoteJid, message.key.remoteJidAlt)
    .some((chatId) => policy.directChatIds.has(chatId));
}

export function inboundPolicySummary(policy) {
  return {
    directChats: policy.directChatIds.size,
    groups: policy.groups.size,
    groupSenders: [...policy.groups.values()]
      .reduce((total, senders) => total + senders.size, 0),
  };
}
