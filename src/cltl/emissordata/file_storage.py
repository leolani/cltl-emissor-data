import glob
import logging
import os
import shutil
from datetime import datetime
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
    def from_config(cls, config_manager: ConfigurationManager, storage: ScenarioStorage = None):
        config = config_manager.get_config("cltl.emissor-data")

        def audio_loader(url, offset, length) -> AudioSource:
            return ClientAudioSource.from_config(config_manager, url, offset, length)

        def image_loader(url) -> ImageSource:
            return ClientImageSource.from_config(config_manager, url)

        return cls(config.get("path"), audio_loader, image_loader, storage)

    def __init__(self, path: str,
                 audio_loader: Callable[[str, int, int], AudioSource],
                 image_loader: Callable[[str], ImageSource],
                 storage: ScenarioStorage):
        self._storage = storage if storage else ScenarioStorage(path)
        self._audio_loader = audio_loader
        self._image_loader = image_loader

        self._controller = None
        self._signals = dict()
        self._signal_idx = dict()

        self._is_modified = False

    def start_scenario(self, scenario: Scenario):
        if self._controller is not None:
            raise ValueError(f"Scenarion {self._controller.scenario.id} is already started, tried to start {scenario.id}")

        self._controller = self._storage.create_scenario(scenario.id, scenario.start,
                                                         scenario.end, scenario.context, scenario.signals)
        self._is_modified = True
        self.flush()

    def update_scenario(self, scenario: Scenario):
        if (not self._controller) or (self._controller.scenario.id != scenario.id):
            raise ValueError(f"Scenario {scenario.id} is not started, current scenario is "
                             f"{self._controller.scenario.id if self._controller else None}")

        self._controller.scenario.context = scenario.context
        self._is_modified = True

    def stop_scenario(self, scenario: Scenario):
        if (not self._controller) or (self._controller.scenario.id != scenario.id):
            raise ValueError(f"Scenario {scenario.id} is not started, current scenario is "
                             f"{self._controller.scenario.id if self._controller else None}")

        self._controller.scenario.ruler.end = scenario.end
        self._is_modified = True
        self.flush()

        self._copy_rdf()

        self._controller = None
        self._signals = dict()

    def _copy_rdf(self):
        start_date = self._to_datetime(self._controller.scenario.ruler.start)
        stop_date = self._to_datetime(self._controller.scenario.ruler.end)

        rdf_path = os.path.join(self._storage.base_path, self._controller.scenario.id, "rdf")
        os.makedirs(rdf_path, exist_ok=True)
        logger.info("Created rdf folder %s for scenario", rdf_path)

        rdf_source = os.path.normpath(os.path.join(self._storage.base_path, "../rdf"))

        log_paths = (os.path.split(path) for path in glob.glob(rdf_source + "/**/brain_log_*.trig", recursive=True))
        log_paths = list((path, filename) for path, filename in log_paths
                    if filename >= "brain_log_" + start_date and filename <= "brain_log_" + stop_date)

        for path, filename in log_paths:
            shutil.copy(os.path.join(path, filename), os.path.join(rdf_path, filename))

        logger.info("Copied rdf logs to scenario %s", self._controller.id)
        logger.debug("Copied rdf logs %s to scenario %s", log_paths, rdf_path)

    def _to_datetime(self, ms):
        return datetime.fromtimestamp(ms / 1000.0).strftime('%Y-%m-%d-%H-%M-%S')

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

        stored_files = None
        if signal.time.end:
            if signal.modality == Modality.TEXT:
                stored_files = []
            elif signal.modality == Modality.AUDIO:
                stored_files = self._store_audio_files(signal)
            elif signal.modality == Modality.IMAGE:
                stored_files = self._store_image_files(signal)
            else:
                logger.error("Skip signal %s with Unsupported modality %s", signal.id, signal.modality)
        signal.files = stored_files

        if signal.id in self._signals:
            self._update(self._signals[signal.id], signal)
        else:
            self._controller.append_signal(signal)
            self._signals[signal.id] = signal
            logger.debug("Added signal id to emissor file storage: %s", signal.id)

        self._is_modified = True

    def _store_audio_files(self, audio_signal: AudioSignal):
        stored = []
        for url in audio_signal.files:
            dest_path, relative_path = self._destination_path(url, "wav")
            try:
                self._store_audio(url, dest_path, audio_signal.ruler)
                logger.info("Copy signal data from %s to %s", url, dest_path)
            except:
                logger.exception("Failed to store %s for audio signal %s", audio_signal.id, url)
            stored.append(relative_path)

        return stored

    def _store_audio(self, url: str, destination: str, segment: MultiIndex):
        start, end = segment.bounds[0], segment.bounds[2]
        with self._audio_loader(url, start, end - start) as source:
            audio = np.concatenate(tuple(source.audio))

        if not audio.dtype == np.int16:
            raise ValueError(f"Wrong sample depth: {audio.dtype}")

        sf.write(str(destination), audio, source.rate)

    def _store_image_files(self, image_signal: ImageSignal):
        stored = []
        for url in image_signal.files:
            dest_path, relative_path = self._destination_path(url, "png")
            try:
                self._store_image(url, dest_path)
                logger.info("Copy signal data from %s to %s", url, dest_path)
            except:
                logger.exception("Failed to store %s for audio signal %s", image_signal.id, url)
            stored.append(relative_path)

        return stored

    def _store_image(self, url: str, destination: str):
        with self._image_loader(url) as source:
            image = source.capture()

        cv2.imwrite(destination, cv2.cvtColor(image.image, cv2.COLOR_RGB2BGR))

    def _destination_path(self, url, postfix):
        relative_path = f"{url.replace('cltl-storage:', '')}.{postfix}"
        dest_path = os.path.join(self._storage.base_path, self._controller.scenario.id, relative_path)

        os.makedirs(os.path.dirname(dest_path), exist_ok=True)

        return os.path.normpath(dest_path), relative_path

    def add_mention(self, mention: Mention):
        if not self._controller:
            logger.warning("Skipping mention %s for stopped Scenario", mention)
            return

        self._add_mention(mention)

    def add_mentions(self, mentions: Iterable[Mention]):
        if not self._controller:
            logger.warning("Skipping mention %s for stopped Scenario", mentions)
            return

        for mention in mentions:
            self._add_mention(mention)

    def _add_mention(self, mention: Mention):
        container_id = mention.segment[0].container_id
        if container_id in self._signals:
            signal_id = container_id
        elif container_id in self._signal_idx:
            signal_id = self._signal_idx[container_id]
        else:
            raise ValueError(
                f"Container {container_id} not found for scenario {self._controller.scenario.id if self._controller else None} and mention {mention.id}")

        self._signals[signal_id].mentions.append(mention)
        self._signal_idx[mention.id] = signal_id
        logger.debug("Added mention id to emissor file storage: %s", mention.id)

        for annotation in mention.annotations:
            if isinstance(annotation, Container):
                self._signal_idx[annotation.id] = signal_id
                logger.debug("Added annotation id to emissor file storage: %s", annotation.id)
            elif isinstance(annotation.value, Container):
                self._signal_idx[annotation.value.id] = signal_id
                logger.debug("Added container id to emissor file storage: %s", annotation.value.id)

        self._is_modified = True

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

    def flush(self):
        if self._controller and self._is_modified:
            self._storage.save_scenario(self._controller)
            self._is_modified = False
            logger.info("Persisted data for scenario %s", self._controller.id)
