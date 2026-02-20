import base64, hashlib
from cryptography.fernet import Fernet
from app.core.config import settings

def _derive_key(secret: str) -> bytes:
    # Fernet requires 32 urlsafe-base64 bytes
    digest = hashlib.sha256(secret.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest)

_fernet = Fernet(_derive_key(settings.app_secret_key))

def encrypt_str(value: str) -> str:
    return _fernet.encrypt(value.encode("utf-8")).decode("utf-8")

def decrypt_str(value: str) -> str:
    return _fernet.decrypt(value.encode("utf-8")).decode("utf-8")
