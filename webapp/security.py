from __future__ import annotations

import base64

from webapp.config import get_secret_key


def _xor_bytes(data: bytes, key: bytes) -> bytes:
    key_len = len(key)
    return bytes([data[i] ^ key[i % key_len] for i in range(len(data))])


def encrypt_text(plain_text: str) -> str:
    raw = plain_text.encode("utf-8")
    key = get_secret_key().encode("utf-8")
    encrypted = _xor_bytes(raw, key)
    return base64.urlsafe_b64encode(encrypted).decode("ascii")


def decrypt_text(cipher_text: str) -> str:
    encrypted = base64.urlsafe_b64decode(cipher_text.encode("ascii"))
    key = get_secret_key().encode("utf-8")
    raw = _xor_bytes(encrypted, key)
    return raw.decode("utf-8")
