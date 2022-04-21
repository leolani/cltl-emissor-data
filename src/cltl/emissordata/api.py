import abc
from typing import Union, Iterable

from emissor.representation.scenario import Signal, Mention, Scenario


class EmissorDataStorage(abc.ABC):
    def start_scenario(self, scenario: Scenario) -> str:
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

    def get_current_scenario_id(self) -> str:
        raise NotImplementedError()