import os
from json import loads
from pathlib import Path

from monzo.authentication import Authentication
from monzo.handlers.filesystem import FileSystem
from monzo_api_wrapper.utils.custom_logger import CustomLogger, loggable

logger = CustomLogger.get_logger()


@loggable
def get_monzo_auth() -> Authentication:
    """Get the Authentication object for authorising requests.

    Returns:
        Authentication object

    """
    path = Path(os.getenv("MONZO_TOKENS_PATH", ""))
    with path.open() as tokens:
        content = loads(tokens.read())

    monzo_auth_obj = Authentication(
        client_id=os.getenv("MONZO_CLIENT_ID", ""),
        client_secret=os.getenv("MONZO_CLIENT_SECRET", ""),
        redirect_url=os.getenv("MONZO_URI_REDIRECT", ""),
        access_token=content["access_token"],
        access_token_expiry=content["expiry"],
        refresh_token=content["refresh_token"],
    )
    logger.info("Authenticating with Monzo API...")

    handler = FileSystem(os.getenv("MONZO_TOKENS_PATH", ""))
    monzo_auth_obj.register_callback_handler(handler)

    return monzo_auth_obj
