import logging

import requests

logger = logging.getLogger(__name__)


class EmissorDataClient:
    def __init__(self, base_url):
        self._url = base_url

    def get_current_scenario_id(self) -> str:
        response = requests.get(f"{self._url}/scenario/current/id")

        if response.ok:
            return response.text
        elif response.status_code == 404:
            return None
        else:
            raise ValueError("Could not retrieve scenario id: (" + str(response.status_code) + ") " + response.text)

    def get_scenario_for_id(self, element_id: str, fallback: bool = True) -> str:
        response = requests.get(f"{self._url}/{element_id}/scenario/id")

        if response.status_code == 404 and fallback:
            logger.warning("Could not find scenario for id %s, fall back to current", element_id)
        elif not response.ok:
            raise ValueError("No such id: " + element_id)

        return response.text