const crypto = require('crypto');

function hashPassword(plainText) {
  if (plainText == null) throw new Error('password is required');
  const password = String(plainText);
  const salt = crypto.randomBytes(16).toString('hex');
  const key = crypto.scryptSync(password, salt, 64);
  return `scrypt$${salt}$${key.toString('hex')}`;
}

function verifyPassword(plainText, storedHash) {
  if (!storedHash) return false;
  const password = String(plainText ?? '');
  const parts = String(storedHash).split('$');
  if (parts.length !== 3) return false;
  const [scheme, salt, expectedHex] = parts;
  if (scheme !== 'scrypt') return false;

  const actual = crypto.scryptSync(password, salt, 64);
  const expected = Buffer.from(expectedHex, 'hex');
  if (expected.length !== actual.length) return false;
  return crypto.timingSafeEqual(expected, actual);
}

module.exports = { hashPassword, verifyPassword };
