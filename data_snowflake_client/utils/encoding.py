import base64

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


def encode_data_base64(text: str) -> str:
    """
    Encode a string to base64.

    Args:
        text (str): The string to encode.

    Returns:
        str: The base64 encoded string.
    """
    text_bytes = text.encode("ascii")
    base64_bytes = base64.b64encode(text_bytes)
    return base64_bytes.decode("ascii")


def decode_data_base64(text: str) -> str:
    """
    Decode a base64 encoded string.

    Args:
        text (str): The base64 encoded string to decode.

    Returns:
        str: The decoded string.
    """
    base64_bytes = text.encode("ascii")
    text_bytes = base64.b64decode(base64_bytes)
    return text_bytes.decode("ascii")


def convert_private_key_to_der(private_key: str) -> bytes:
    """
    Convert a private key in string format to DER format.

    DER (Distinguished Encoding Rules) is a binary format used to encode
    cryptographic keys and certificates.

    Args:
        private_key (str): A private key.

    Returns:
        bytes: The private key in DER format.
    """

    # Load the PEM-encoded private key
    private_key_obj = serialization.load_pem_private_key(
        private_key.encode("utf-8"), password=None, backend=default_backend()
    )

    # Convert the private key to DER format
    private_key_der = private_key_obj.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    return private_key_der
