import logging
import os
import shutil
from typing import Iterable

from emissor.persistence import ScenarioStorage
from emissor.representation.util import marshal, unmarshal
from emissor.representation.container import Container
from emissor.representation.scenario import Mention, Signal, Scenario, Modality

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

        self._controller = self._storage.create_scenario(scenario.id, scenario.start,
                                                         scenario.end, scenario.context, scenario.signals)

    def update_scenario(self, scenario: Scenario):
        if self._controller.scenario.id != scenario.id:
            raise ValueError(f"Scenario {scenario.id} is not started, current scenario is "
                             f"{self._controller.scenario.id if self._controller else None}")

        self._controller.scenario.context = scenario.context

        self._storage.save_scenario(self._controller)

    def stop_scenario(self, scenario: Scenario):
        if self._controller.scenario.id != scenario.id:
            raise ValueError(f"Scenario {scenario.id} is not started, current scenario is "
                             f"{self._controller.scenario.id if self._controller else None}")

        self._controller.scenario.ruler.end = scenario.end
        self._storage.save_scenario(self._controller)

        self._controller = None
        self._signals = dict()

    def add_signal(self, signal: Signal):
        try:
            signal = unmarshal(marshal(signal, cls=signal.__class__), cls=signal.__class__)
        except Exception as e:
            logger.exception("Serialization failed for %s", signal)
            raise e

        if not self._controller:
            logger.warning(f"Skipping signal for stopped Scenario {signal.ruler.container_id}")
            return

        if self._controller.scenario.id != signal.time.container_id:
            raise ValueError(f"Scenario {signal.ruler.container_id} is not the current scenario ({self._controller.scenario.id if self._controller else None}) for signal {signal}")

        try:
            if signal.time.end and signal.modality != Modality.TEXT:
                # TODO copy independent of file system, i.e. read and write data
                postfix = ""
                if signal.modality == Modality.AUDIO:
                    postfix = ".wav"
                elif signal.modality == Modality.IMAGE:
                    postfix = ".png"

                signal.files = [f"{file.replace('cltl-storage:', '')}{postfix}" for file in signal.files]
                for file in signal.files:
                    src_path = os.path.normpath(os.path.join("storage", file))
                    dest_path = os.path.normpath(os.path.join(self._storage.base_path, self._controller.scenario.id, file))
                    dest_path = str(dest_path).replace("video", "image")
                    logger.info("Copy signal data from %s to %s", src_path, dest_path)
                    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                    shutil.copy2(src_path, dest_path)
        except:
            logger.exception("Copying signal failed")

        if signal.id in self._signals:
            self._update(self._signals[signal.id], signal)
        else:
            self._controller.append_signal(signal)
            self._signals[signal.id] = signal

        self._storage.save_scenario(self._controller)

    def add_mention(self, mention: Mention):
        if not self._controller:
            logger.warning("Skipping mention %s for stopped Scenario", mention)
            return

        self._add_mention(mention)
        self._storage.save_scenario(self._controller)

    def add_mentions(self, mentions: Iterable[Mention]):
        if not self._controller:
            logger.warning("Skipping mention %s for stopped Scenario", mentions)
            return

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
        self._signal_idx[mention.id] = signal_id

        for annotation in mention.annotations:
            if isinstance(annotation, Container):
                self._signal_idx[annotation.id] = signal_id
            elif isinstance(annotation.value, Container):
                self._signal_idx[annotation.value.id] = signal_id

    def _update(self, obj, update_obj):
        for key, value in vars(update_obj).items():
            if hasattr(obj, key) and value:
                setattr(obj, key, value)

    def get_signal(self, signal_id: str) -> Signal:
        return self._signals[signal_id]

    def get_current_scenario_id(self) -> str:
        return self._controller.scenario.id if self._controller else None

    def get_scenario_for_id(self, element_id: str) -> str:
        if element_id in self._signals:
            signal = self._signals[element_id]
        else:
            signal_id = self._signal_idx[element_id]
            signal = self._signals[signal_id]

        return signal.time.container_id
