import requests


class EmissorDataClient:
    def __init__(self, base_url):
        self._url = base_url

    def get_current_scenario_id(self) -> str:
        return requests.get(f"{self._url}/scenario/current/id").text

    def get_scenario_for_id(self, element_id: str) -> str:
        return requests.get(f"{self._url}/{element_id}/scenario/id").text
