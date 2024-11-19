import json
import os
from typing import Optional

import requests
from monzo_api_wrapper.utils import sql_templates
from monzo_api_wrapper.utils.custom_logger import CustomLogger, loggable
from monzo_api_wrapper.utils.db import Db

logger = CustomLogger.get_logger()


class APIError(Exception):
    """Custom exception for API-related errors."""

    def __init__(self, message: str, response_data: Optional[dict] = None) -> None:
        """Initialize an APIError instance.

        Args:
            message (str): The error message describing the issue.
            response_data (Optional[dict]): Additional data from the API response that provides context for the error. Defaults to None.

        """
        super().__init__(message)
        self.response_data = response_data


@loggable
def upload_new_lunch_money_transactions(
    db: Db, transactions_dct_lst: list | dict, chunk_size: int = 1
) -> None:
    """Uploads new transactions to the Lunch Money API.

    This function takes a list or dictionary of transaction data and uploads it to the Lunch Money API.
    If the input is a list, it uploads in chunks for efficient processing. If the input is a dictionary,
    it wraps it in a list and sends it as a single transaction. It also updates the local database with
    the Lunch Money transaction IDs after a successful upload.

    Args:
        db (Db): Database connection object.
        transactions_dct_lst (Union[dict, list]): A dictionary or list of transaction data to upload.
        chunk_size (int, optional): The number of transactions to upload per request when input is a list.
            Defaults to 1.

    Raises:
        Exception: When environment variables for URL and tokens are missing.

    """
    access_token = os.getenv("LUNCH_MONEY_ACCESS_TOKEN")
    api_url = os.getenv("LUNCH_MONEY_BASE_API_URL", "") + "transactions"

    if not access_token or not api_url:
        logger.error(
            "LUNCH_MONEY_ACCESS_TOKEN or LUNCH_MONEY_API_URL environment variables are missing."
        )
        raise Exception("Environment variables for API access are missing.")

    if isinstance(transactions_dct_lst, list):
        for start in range(0, len(transactions_dct_lst), chunk_size):
            chunk = transactions_dct_lst[start : start + chunk_size]
            process_transaction_chunk(db=db, chunk=chunk)
    else:
        process_transaction_chunk(db=db, chunk=[transactions_dct_lst])


@loggable
def process_transaction_chunk(db: Db, chunk: list[dict]) -> None:
    """Processes and uploads a single chunk of transactions to the Lunch Money API.

    This helper function sends a chunk of transactions as a POST request to the Lunch Money API.
    It also updates the local database with the returned Lunch Money ID for each transaction.

    Args:
        db (Db): Database connection object.
        chunk (List[dict]): A chunk of transactions to be uploaded.

    Raises:
        APIError exception is raised if response is an error.

    """
    headers = {
        "Authorization": f"Bearer {os.getenv('LUNCH_MONEY_ACCESS_TOKEN')}",
        "Content-Type": "application/json",
    }

    try:
        response = requests.post(
            os.getenv("LUNCH_MONEY_BASE_API_URL", "") + "transactions",
            headers=headers,
            data=json.dumps({"transactions": chunk, "apply_rules": True}),
            timeout=10,
        )
        response.raise_for_status()  # Raises HTTPError for non-2xx responses
        response_data = response.json()

        # Check if there's an error in the response
        if "error" in response_data:
            logger.error(f"API error response: {response_data}")
            raise APIError("API responded with an error", response_data=response_data)

        # Process successful response
        if "ids" in response_data:
            lunch_money_id = response_data["ids"][0]
            lunch_money_id_monzo_id_map = {chunk[0]["external_id"]: lunch_money_id}
            update_db_transactions_id(db, lunch_money_id_monzo_id_map)
            logger.debug(f"Updated database with new Lunch Money ID for chunk: {chunk}")
        else:
            logger.warning(f"Unexpected response structure: {response_data}")
    except APIError as e:
        logger.exception(f"API error: {e}", extra={"response_data": e.response_data})
    except requests.RequestException as e:
        logger.exception(f"Request failed: {e}", exc_info=True, extra={"chunk": chunk})


@loggable
def update_db_transactions_id(db: Db, transactions_id_map: dict[str, int]) -> None:
    """Update the database with Lunch Money transaction IDs.

    For each transaction, this function updates the `lunch_money_id` in the database for the given
    `external_id`.

    Args:
        db (Db): Database connection object.
        transactions_id_map (Dict[str, int]): A mapping of `external_id` to `lunch_money_id`.

    """
    for external_id, lunch_money_id in transactions_id_map.items():
        db.query(
            sql=sql_templates.add_id.format(
                table=os.getenv("DB_TABLE"),
                lunch_money_id=lunch_money_id,
                external_id=external_id,
            ),
            return_data=False,
        )
