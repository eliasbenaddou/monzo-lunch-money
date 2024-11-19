import json
import os

import requests
from monzo_api_wrapper.utils.custom_logger import CustomLogger, loggable

logger = CustomLogger.get_logger()


class UpdateChangedTransactionsError(Exception):
    """Custom exception for errors while updating changed transactions."""

    pass


@loggable
def send_payload(url: str, headers: dict, payload: dict) -> None:
    """Send the payload over the Lunch Money API.

    Args:
        url (str): URL for Lunch Money transactions
        headers (dict): Headers for authentication
        payload (dict): Data to send in the payload

    Raises:
        UpdateChangedTransactionsError: When environment variables for URL and tokens are missing.

    """
    try:
        response = requests.put(url, headers=headers, data=json.dumps(payload), timeout=10)
        response.raise_for_status()
        logger.info(f"{response.text}")
    except requests.RequestException as e:
        logger.exception("Request failed", exc_info=True)
        raise UpdateChangedTransactionsError("Failed to update transaction in Lunch Money") from e


@loggable
def update_changed_lunch_money_transactions(transactions_dct_lst: list | dict) -> None:
    """Updates changed transactions to the Lunch Money API.

    This function takes a list or dictionary of transaction data and uploads it to the Lunch Money API.
    If the input is a list, it updates sequentially. If the input is a dictionary,
    it wraps it in a list and sends it as a single transaction.

    Args:
        transactions_dct_lst (Union[dict, list]): A dictionary or list of transaction data to upload.
        chunk_size (int, optional): The number of transactions to upload per request when input is a list.
            Defaults to 1.

    """
    base_url = os.getenv("LUNCH_MONEY_BASE_API_URL", "")
    headers = {
        "Authorization": f"Bearer {os.getenv('LUNCH_MONEY_ACCESS_TOKEN', '')}",
        "Content-Type": "application/json",
    }

    if isinstance(transactions_dct_lst, list):
        chunk_size = 1
        for start in range(0, len(transactions_dct_lst), chunk_size):
            chunk = transactions_dct_lst[start : start + chunk_size]
            logger.debug(f"chunk {chunk[0]}")
            lunch_money_id = chunk[0]["lunch_money_id"]
            url = base_url + f"transactions/{lunch_money_id}"
            payload = {"transaction": chunk[0]}
            send_payload(url=url, headers=headers, payload=payload)
    else:
        lunch_money_id = transactions_dct_lst["transaction_id"]
        url = base_url + f"transactions/{lunch_money_id}"
        payload = {"transaction": transactions_dct_lst}
        send_payload(url=url, headers=headers, payload=payload)
