import abc
import io
from typing import Union, Iterable, Optional, List, Dict, Any

from emissor.representation.scenario import Signal, Mention, Scenario


class EmissorDataStorage(abc.ABC):
    def start_scenario(self, scenario: Scenario) -> str:
        raise NotImplementedError()

    def update_scenario(self, scenario: Scenario) -> str:
        raise NotImplementedError()

    def stop_scenario(self, scenario: Scenario) -> str:
        raise NotImplementedError()

    def add_signal(self, signal: Signal):
        raise NotImplementedError()

    def add_mention(self, mention: Mention):
        raise NotImplementedError()

    def add_mentions(self, mentions: Iterable[Mention]):
        raise NotImplementedError()

    def get_signal(self, signal_id: str) -> Signal:
        raise NotImplementedError()

    def get_scenario_for_id(self, element_id: str) -> str:
        raise NotImplementedError()

    def flush(self) -> str:
        pass

    def get_storage_path(self) -> Optional[str]:
        """Return the base storage path for static file serving, or None if not applicable."""
        raise NotImplementedError()

    def list_scenarios(self) -> List[Dict[str, Any]]:
        """List all scenarios with their metadata (id, start, end, context)."""
        raise NotImplementedError()

    def get_scenario(self, scenario_id: str) -> Dict[str, Any]:
        """Get a scenario with all signals loaded. Returns dict with 'scenario' and 'signals' keys."""
        raise NotImplementedError()

    def create_scenario_zip(self, scenario_id: str) -> io.BytesIO:
        """Create a zip file of a scenario directory. Returns BytesIO buffer."""
        raise NotImplementedError()