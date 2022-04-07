import logging
from typing import Iterable

from emissor.persistence import ScenarioStorage
from emissor.representation.scenario import Mention, Signal, Scenario

from cltl.emissordata.api import EmissorDataStorage

logger = logging.getLogger(__name__)


class EmissorDataFileStorage(EmissorDataStorage):
    def __init__(self, path: str):
        self._storage = ScenarioStorage(path)
        self._controller = None
        self._signals = dict()

    def start_scenario(self, scenario: Scenario):
        if self._controller is not None:
            raise ValueError(f"Scenarion {self._controller.scenario.id} is already started, tried to start {scenario.id}")

        signals = scenario.signals if scenario.signals else []
        self._controller = self._storage.create_scenario(scenario.id, scenario.start,
                                                         None, scenario.context, signals)

    def stop_scenario(self, scenario: Scenario):
        if self._controller.scenario.id != scenario.id:
            raise ValueError(f"Scenario {scenario.id} is not started, current scenario is "
                             f"{self._controller.scenario.id if self._controller else None}")

        self._controller.scenario.end = scenario.end
        self._storage.save_scenario(self._controller)

        self._controller = None
        self._signals = dict()

    def add_signal(self, signal: Signal):
        if not self._controller:
            raise ValueError(f"Scenario {signal.ruler.container_id} is not the current scenario (None)")

        if self._controller.scenario.id != signal.ruler.container_id:
            raise ValueError(f"Scenario {signal.ruler.container_id} is not the current scenario ({self._controller.scenario.id if self._controller else None})")

        self._controller.append_signal(signal)
        self._storage.save_scenario(self._controller)
        self._signals[signal.id] = signal

    def add_mention(self, mention: Mention):
        self._add_mention(mention)
        self._storage.save_scenario(self._controller)

    def add_mentions(self, mentions: Iterable[Mention]):
        for mention in mentions:
            self._add_mention(mention)

        self._storage.save_scenario(self._controller)

    def _add_mention(self, mention: Mention):
        signal_id = mention.segment[0].container_id
        if not signal_id in self._signals:
            raise ValueError(f"Signal {signal_id} not found for scenario {self._controller.scenario.id if self._controller else None}")

        self._signals[signal_id].mentions.append(mention)