"""
encrypt_decrypt.py
==================

Purpose:
This module provides basic AES encryption and decryption utilities using
CBC mode and PBKDF2 key derivation. It is used for encrypting sensitive
data such as AWS keys before storing them and decrypting when required.

Dependencies:
- pycryptodome (for AES and PBKDF2)
- base64
"""

import base64
from Cryptodome.Cipher import AES
from Cryptodome.Protocol.KDF import PBKDF2
import os
import sys
from resources.dev import config

# --------------------------------------------------------------------
# Fetch encryption parameters from config
# --------------------------------------------------------------------
try:
    key = config.key
    iv = config.iv
    salt = config.salt

    if not (key and iv and salt):
        raise Exception("Error while fetching details for key/iv/salt")
except Exception as e:
    print(f"Error occurred. Details: {e}")
    sys.exit(0)

# Block size for AES
BS = 16

# Padding and unpadding functions for AES CBC
pad = lambda s: bytes(s + (BS - len(s) % BS) * chr(BS - len(s) % BS), 'utf-8')
unpad = lambda s: s[0:-ord(s[-1:])]


# --------------------------------------------------------------------
# Key Derivation
# --------------------------------------------------------------------
def get_private_key():
    """
    Generate a 32-byte private key using PBKDF2 from config.key and config.salt.

    Returns:
        key32 (bytes): 32-byte derived key for AES
    """
    Salt = salt.encode('utf-8')
    kdf = PBKDF2(key, Salt, 64, 1000)
    key32 = kdf[:32]
    return key32


# --------------------------------------------------------------------
# Encryption Function
# --------------------------------------------------------------------
def encrypt(raw):
    """
    Encrypt a raw string using AES CBC mode and return Base64 encoded result.

    Args:
        raw (str): Plain text string to encrypt

    Returns:
        str: Base64 encoded encrypted string
    """
    raw = pad(raw)
    cipher = AES.new(get_private_key(), AES.MODE_CBC, iv.encode('utf-8'))
    return base64.b64encode(cipher.encrypt(raw))


# --------------------------------------------------------------------
# Decryption Function
# --------------------------------------------------------------------
def decrypt(enc):
    """
    Decrypt a Base64 encoded AES-encrypted string.

    Args:
        enc (str): Base64 encoded encrypted string

    Returns:
        str: Decrypted plain text
    """
    cipher = AES.new(get_private_key(), AES.MODE_CBC, iv.encode('utf-8'))
    return unpad(cipher.decrypt(base64.b64decode(enc))).decode('utf8')
