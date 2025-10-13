import logging

import requests

logger = logging.getLogger(__name__)


class EmissorDataClient:
    def __init__(self, base_url):
        self._url = base_url

    def get_scenario_for_id(self, element_id: str) -> str:
        response = requests.get(f"{self._url}/{element_id}/scenario/id")

        if not response.ok:
            raise ValueError("No such id: " + element_id)

        return response.text