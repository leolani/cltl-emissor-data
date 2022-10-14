import logging
import os
from typing import Iterable, Callable

import cv2
import numpy as np
import soundfile as sf
from cltl.backend.source.client_source import ClientAudioSource, ClientImageSource
from cltl.backend.spi.audio import AudioSource
from cltl.backend.spi.image import ImageSource
from cltl.combot.infra.config import ConfigurationManager
from emissor.persistence import ScenarioStorage
from emissor.representation.container import Container, MultiIndex
from emissor.representation.scenario import Mention, Signal, Scenario, Modality, AudioSignal, ImageSignal
from emissor.representation.util import marshal, unmarshal

from cltl.emissordata.api import EmissorDataStorage

logger = logging.getLogger(__name__)


class EmissorDataFileStorage(EmissorDataStorage):
    @classmethod
    def from_config(cls, config_manager: ConfigurationManager):
        config = config_manager.get_config("cltl.emissor-data")

        def audio_loader(url, offset, length) -> AudioSource:
            return ClientAudioSource.from_config(config_manager, url, offset, length)

        def image_loader(url) -> ImageSource:
            return ClientImageSource.from_config(config_manager, url)

        return cls(config.get("path"), audio_loader, image_loader)

    def __init__(self, path: str,
                 audio_loader: Callable[[str, int, int], AudioSource],
                 image_loader: Callable[[str], ImageSource]):
        self._storage = ScenarioStorage(path)
        self._audio_loader = audio_loader
        self._image_loader = image_loader

        self._controller = None
        self._signals = dict()
        self._signal_idx = dict()

    def start_scenario(self, scenario: Scenario):
        if self._controller is not None:
            raise ValueError(f"Scenarion {self._controller.scenario.id} is already started, tried to start {scenario.id}")

        self._controller = self._storage.create_scenario(scenario.id, scenario.start,
                                                         scenario.end, scenario.context, scenario.signals)

    def update_scenario(self, scenario: Scenario):
        if (not self._controller) or (self._controller.scenario.id != scenario.id):
            raise ValueError(f"Scenario {scenario.id} is not started, current scenario is "
                             f"{self._controller.scenario.id if self._controller else None}")

        self._controller.scenario.context = scenario.context

        self._storage.save_scenario(self._controller)

    def stop_scenario(self, scenario: Scenario):
        if (not self._controller) or (self._controller.scenario.id != scenario.id):
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

        if signal.time.end:
            if signal.modality == Modality.TEXT:
                pass
            elif signal.modality == Modality.AUDIO:
                self._store_audio_files(signal)
            elif signal.modality == Modality.IMAGE:
                self._store_image_files(signal)
            else:
                logger.error("Skip signal %s with Unsupported modality %s", signal.id, signal.modality)

        if signal.id in self._signals:
            self._update(self._signals[signal.id], signal)
        else:
            self._controller.append_signal(signal)
            self._signals[signal.id] = signal

        self._storage.save_scenario(self._controller)

    def _store_audio_files(self, audio_signal: AudioSignal):
        for url in audio_signal.files:
            dest_path = self._destination_path(url, "wav")
            try:
                self._store_audio(url, dest_path, audio_signal.ruler)
                logger.info("Copy signal data from %s to %s", url, dest_path)
            except:
                logger.exception("Failed to store %s for audio signal %s", audio_signal.id, url)

    def _store_audio(self, url: str, destination: str, segment: MultiIndex):
        start, end = segment.bounds[0], segment.bounds[2]
        with self._audio_loader(url, start, end - start) as source:
            audio = np.concatenate(tuple(source.audio))

        if not audio.dtype == np.int16:
            raise ValueError(f"Wrong sample depth: {audio.dtype}")

        sf.write(str(destination), audio, source.rate)

    def _store_image_files(self, image_signal: ImageSignal):
        for url in image_signal.files:
            dest_path = self._destination_path(url, "png")
            try:
                self._store_image(url, dest_path)
                logger.info("Copy signal data from %s to %s", url, dest_path)
            except:
                logger.exception("Failed to store %s for audio signal %s", image_signal.id, url)

    def _store_image(self, url: str, destination: str):
        with self._image_loader(url) as source:
            image = source.capture()

        cv2.imwrite(destination, cv2.cvtColor(image.image, cv2.COLOR_RGB2BGR))

    def _destination_path(self, url, postfix):
        file_name = f"{url.replace('cltl-storage:', '')}.{postfix}"
        dest_path = os.path.join(self._storage.base_path, self._controller.scenario.id, file_name)

        os.makedirs(os.path.dirname(dest_path), exist_ok=True)

        return os.path.normpath(dest_path)

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
