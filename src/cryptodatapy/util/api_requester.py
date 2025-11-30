import requests
import logging
from time import sleep
from typing import Dict, Any, Union, Optional

logger = logging.getLogger(__name__)


class APIRequester:
    """
    A robust client for making external API GET requests.

    Encapsulates retry logic, error handling, and throttling based on
    parameters supplied by the DataRequest object.
    """

    @staticmethod
    def get_request(
            url: str,
            params: Dict[str, Union[str, int]],
            headers: Optional[Dict[str, str]] = None,
            trials: int = 3,
            pause: float = 0.1
    ) -> Optional[Dict[str, Any]]:
        """
        Submits a resilient GET request to the API with retry logic.

        Parameters
        ----------
        url : str
            The target endpoint URL for the GET request.
        params : dict
            Dictionary containing query parameters for the request.
        headers : dict, optional
            Dictionary containing HTTP headers for the request.
        trials : int, default=3
            Maximum number of attempts to make for the request.
        pause : float, default=0.1
            Number of seconds to pause between failed attempts.

        Returns
        -------
        Optional[Dict[str, Any]]
            Data response in JSON format (dict) if successful, otherwise None.
        """
        # set number of attempts
        attempts = 0
        resp = None

        while attempts < trials:
            try:
                resp = requests.get(url, params=params, headers=headers)
                resp.raise_for_status()
                return resp.json()

            # handle HTTP errors
            except requests.exceptions.HTTPError as http_err:
                status_code = resp.status_code
                log_msg = f"HTTP Error ({status_code}): {http_err}"

                if status_code == 400:
                    logger.warning(f"Bad Request (400): {log_msg}")  # Warning: Adapter/Input issue
                elif status_code == 401:
                    logger.error(f"Unauthorized (401): {log_msg}")  # Error: Configuration issue
                elif status_code == 403:
                    logger.error(f"Forbidden (403): {log_msg}")  # Error: Configuration issue
                elif status_code == 404:
                    logger.warning(f"Not Found (404): {log_msg}")  # Warning: Resource not found/Input issue
                elif status_code >= 500:
                    logger.error(f"Server Error: {log_msg}")  # Error: System failure
                else:
                    logger.warning(log_msg)

            except requests.exceptions.RequestException as req_err:
                # Handle non-HTTP exceptions (e.g., network issues, timeouts)
                logger.warning(f"Network/Request error: {req_err}")  # Warning: Transient issue

            except Exception as e:
                # Handle unexpected errors (e.g., JSON parsing failure)
                logger.error(f"Unexpected error during API call: {e}")  # Error: Unhandled code failure

            # retry Logic
            attempts += 1
            if attempts < trials:
                logger.info(f"Retrying attempt #{attempts + 1}/{trials} after {pause} seconds...")  # Info: Normal trace
                sleep(pause)
            else:
                logger.error(f"Max attempts ({trials}) reached. Unable to fetch data from {url}.")  # Error: failure
                break

        # failure case
        return None
