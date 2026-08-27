import assert from 'node:assert/strict';
import test from 'node:test';

import {
  inboundAllowed,
  inboundPolicySummary,
  parseInboundPolicy,
} from '../src/inbound-policy.js';

const direct = {
  key: { remoteJid: 'alice@s.whatsapp.net', id: 'direct-1' },
  message: { conversation: 'hello' },
};
const group = {
  key: {
    remoteJid: 'team@g.us',
    participant: 'alice@s.whatsapp.net',
    id: 'group-1',
  },
  message: { conversation: 'hello team' },
};

test('missing and empty policy deny every inbound identity', () => {
  for (const policy of [
    parseInboundPolicy(undefined),
    parseInboundPolicy({}),
    parseInboundPolicy({ directChatIds: [], groups: [] }),
  ]) {
    assert.equal(inboundAllowed(direct, policy), false);
    assert.equal(inboundAllowed(group, policy), false);
  }
});

test('exact direct and group-sender rules accept primary or alternate Baileys JIDs', () => {
  const policy = parseInboundPolicy({
    directChatIds: ['alice@s.whatsapp.net', 'alice@lid'],
    groups: [{
      chatId: 'team@g.us',
      senderIds: ['alice@s.whatsapp.net', 'alice@lid'],
    }],
  });
  assert.equal(inboundAllowed(direct, policy), true);
  assert.equal(inboundAllowed({
    ...direct,
    key: { ...direct.key, remoteJid: 'opaque@lid', remoteJidAlt: 'alice@s.whatsapp.net' },
  }, policy), true);
  assert.equal(inboundAllowed(group, policy), true);
  assert.equal(inboundAllowed({
    ...group,
    key: { ...group.key, participant: 'opaque@lid', participantAlt: 'alice@s.whatsapp.net' },
  }, policy), true);
  assert.deepEqual(inboundPolicySummary(policy), {
    directChats: 2,
    groups: 1,
    groupSenders: 2,
  });
});

test('a group requires both the exact conversation and an exact sender', () => {
  const policy = parseInboundPolicy({
    groups: [{ chatId: 'team@g.us', senderIds: ['alice@s.whatsapp.net'] }],
  });
  assert.equal(inboundAllowed({
    ...group,
    key: { ...group.key, remoteJid: 'other@g.us' },
  }, policy), false);
  assert.equal(inboundAllowed({
    ...group,
    key: { ...group.key, participant: 'mallory@s.whatsapp.net' },
  }, policy), false);
  assert.equal(inboundAllowed({ ...group, key: { ...group.key, fromMe: true } }, policy), false);
});

test('malformed, ambiguous and oversized policies fail initialization', () => {
  for (const value of [
    null,
    { unknown: true },
    { directChatIds: ['alice@s.whatsapp.net', 'alice@s.whatsapp.net'] },
    { directChatIds: ['team@g.us'] },
    { groups: [{ chatId: 'team@g.us', senderIds: [] }] },
    { groups: [{ chatId: 'not-a-group', senderIds: ['alice@s.whatsapp.net'] }] },
    { groups: [
      { chatId: 'team@g.us', senderIds: ['alice@s.whatsapp.net'] },
      { chatId: 'team@g.us', senderIds: ['bob@s.whatsapp.net'] },
    ] },
    { directChatIds: Array.from({ length: 1_025 }, (_, index) => `${index}@lid`) },
  ]) {
    assert.throws(() => parseInboundPolicy(value));
  }
});
