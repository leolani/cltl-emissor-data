import logging
from typing import Iterable

from emissor.persistence import ScenarioStorage
from emissor.representation.scenario import Mention, Signal, Scenario
from emissor.representation.container import Container

from cltl.emissordata.api import EmissorDataStorage

logger = logging.getLogger(__name__)


class EmissorDataFileStorage(EmissorDataStorage):
    def __init__(self, path: str):
        self._storage = ScenarioStorage(path)
        self._controller = None
        self._signals = dict()
        self._signal_idx = dict()

    def start_scenario(self, scenario: Scenario):
        if self._controller is not None:
            raise ValueError(f"Scenarion {self._controller.scenario.id} is already started, tried to start {scenario.id}")

        signals = scenario.signals if scenario.signals else []
        self._controller = self._storage.create_scenario(scenario.id, scenario.start,
                                                         scenario.end, scenario.context, signals)

    def stop_scenario(self, scenario: Scenario):
        if self._controller.scenario.id != scenario.id:
            raise ValueError(f"Scenario {scenario.id} is not started, current scenario is "
                             f"{self._controller.scenario.id if self._controller else None}")

        self._controller.scenario.ruler.end = scenario.end
        self._storage.save_scenario(self._controller)

        self._controller = None
        self._signals = dict()

    def add_signal(self, signal: Signal):
        if not self._controller:
            raise ValueError(f"Scenario {signal.ruler.container_id} is not the current scenario (None) for signal {signal}")

        if self._controller.scenario.id != signal.time.container_id:
            raise ValueError(f"Scenario {signal.ruler.container_id} is not the current scenario ({self._controller.scenario.id if self._controller else None}) for signal {signal}")

        if signal.id in self._signals:
            self._update(self._signals[signal.id], signal)
        else:
            self._controller.append_signal(signal)
            self._signals[signal.id] = signal

        self._storage.save_scenario(self._controller)

    def add_mention(self, mention: Mention):
        self._add_mention(mention)
        self._storage.save_scenario(self._controller)

    def add_mentions(self, mentions: Iterable[Mention]):
        for mention in mentions:
            self._add_mention(mention)

        self._storage.save_scenario(self._controller)

    def _add_mention(self, mention: Mention):
        container_id = mention.segment[0].container_id
        if container_id in self._signals:
            signal_id = container_id
        elif container_id in self._signal_idx:
            signal_id = self._signal_idx[container_id]
        else:
            raise ValueError(f"Container {container_id} not found for scenario {self._controller.scenario.id if self._controller else None}")

        self._signals[signal_id].mentions.append(mention)

        for annotation in mention.annotations:
            if isinstance(annotation, Container):
                self._signal_idx[annotation.id] = signal_id
            elif isinstance(annotation.value, Container):
                self._signal_idx[annotation.value.id] = signal_id

    def _update(self, obj, update_obj):
        for key, value in vars(update_obj).items():
            if hasattr(obj, key) and value:
                setattr(obj, key, value)