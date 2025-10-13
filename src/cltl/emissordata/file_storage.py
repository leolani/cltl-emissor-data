import glob
import logging
import os
import shutil
from datetime import datetime
from typing import Iterable, Callable, Any

import numpy as np

from cltl.combot.infra.config import ConfigurationManager
from emissor.persistence import ScenarioStorage
from emissor.representation.container import Container, MultiIndex
from emissor.representation.scenario import Mention, Signal, Scenario, Modality, AudioSignal, ImageSignal
from emissor.representation.util import marshal, unmarshal

from cltl.emissordata.api import EmissorDataStorage

logger = logging.getLogger(__name__)

try:
    import soundfile as sf
except ImportError as e:
        logger.warning("Import failed: %s", e)

try:
    import cv2
except ImportError as e:
    logger.warning("Import failed: %s", e)


class EmissorDataFileStorage(EmissorDataStorage):
    @classmethod
    def from_config(cls, config_manager: ConfigurationManager, storage: ScenarioStorage = None):
        config = config_manager.get_config("cltl.emissor-data")

        try:
            from cltl.backend.source.client_source import ClientAudioSource
            from cltl.backend.spi.audio import AudioSource
            import soundfile as sf

            def audio_loader(url, offset, length) -> AudioSource:
                return ClientAudioSource.from_config(config_manager, url, offset, length)
        except ImportError as e:
            audio_loader = None
            logger.warning("Could not create Audio Source: %s", e)

        try:
            from cltl.backend.source.client_source import ClientImageSource
            from cltl.backend.spi.image import ImageSource
            import cv2

            def image_loader(url) -> ImageSource:
                return ClientImageSource.from_config(config_manager, url)
        except ImportError as e:
            image_loader = None
            logger.warning("Could not create Image Source: %s", e)

        return cls(config.get("path"), audio_loader, image_loader, storage)

    def __init__(self, path: str,
                 audio_loader: Callable[[str, int, int], Any],
                 image_loader: Callable[[str], Any],
                 storage: ScenarioStorage):
        self._storage = storage if storage else ScenarioStorage(path)
        self._audio_loader = audio_loader
        self._image_loader = image_loader

        # Support multiple concurrent scenarios
        self._controllers = dict()  # scenario_id -> controller
        self._signals = dict()  # scenario_id -> {signal_id -> signal}
        self._signal_idx = dict()  # scenario_id -> {element_id -> signal_id}
        self._is_modified = dict()  # scenario_id -> bool

    def start_scenario(self, scenario: Scenario):
        scenario_id = scenario.id
        if scenario_id in self._controllers:
            raise ValueError(f"Scenario {scenario_id} is already started")

        controller = self._storage.create_scenario(scenario.id, scenario.start,
                                                   scenario.end, scenario.context, scenario.signals)
        self._controllers[scenario_id] = controller
        self._signals[scenario_id] = dict()
        self._signal_idx[scenario_id] = dict()
        self._is_modified[scenario_id] = True
        self._flush_scenario(scenario_id)
        logger.info("Started scenario %s", scenario_id)

    def update_scenario(self, scenario: Scenario):
        scenario_id = scenario.id
        if scenario_id not in self._controllers:
            raise ValueError(f"Scenario {scenario_id} is not started")

        self._controllers[scenario_id].scenario.context = scenario.context
        self._is_modified[scenario_id] = True

    def stop_scenario(self, scenario: Scenario):
        scenario_id = scenario.id
        if scenario_id not in self._controllers:
            raise ValueError(f"Scenario {scenario_id} is not started")

        controller = self._controllers[scenario_id]
        controller.scenario.ruler.end = scenario.end
        self._is_modified[scenario_id] = True
        self._flush_scenario(scenario_id)

        self._copy_rdf(scenario_id)

        # Clean up scenario data
        del self._controllers[scenario_id]
        del self._signals[scenario_id]
        del self._signal_idx[scenario_id]
        del self._is_modified[scenario_id]
        logger.info("Stopped scenario %s", scenario_id)

    def _copy_rdf(self, scenario_id: str):
        controller = self._controllers[scenario_id]
        start_date = self._to_datetime(controller.scenario.ruler.start)
        stop_date = self._to_datetime(controller.scenario.ruler.end)

        rdf_path = os.path.join(self._storage.base_path, scenario_id, "rdf")
        os.makedirs(rdf_path, exist_ok=True)
        logger.info("Created rdf folder %s for scenario", rdf_path)

        rdf_source = os.path.normpath(os.path.join(self._storage.base_path, "../rdf"))

        log_paths = (os.path.split(path) for path in glob.glob(rdf_source + "/**/brain_log_*.trig", recursive=True))
        log_paths = list((path, filename) for path, filename in log_paths
                    if filename >= "brain_log_" + start_date and filename <= "brain_log_" + stop_date)

        for path, filename in log_paths:
            shutil.copy(os.path.join(path, filename), os.path.join(rdf_path, filename))

        logger.info("Copied rdf logs to scenario %s", scenario_id)
        logger.debug("Copied rdf logs %s to scenario %s", log_paths, rdf_path)

    def _to_datetime(self, ms):
        return datetime.fromtimestamp(ms / 1000.0).strftime('%Y-%m-%d-%H-%M-%S')

    def add_signal(self, signal: Signal):
        try:
            signal = unmarshal(marshal(signal, cls=signal.__class__), cls=signal.__class__)
        except Exception as e:
            logger.exception("Serialization failed for %s", signal)
            raise e

        scenario_id = signal.time.container_id
        if scenario_id not in self._controllers:
            logger.warning(f"Skipping signal for stopped or unknown Scenario {scenario_id}")
            return

        stored_files = None
        if signal.time.end:
            if signal.modality == Modality.TEXT:
                stored_files = []
            elif signal.modality == Modality.AUDIO:
                stored_files = self._store_audio_files(signal, scenario_id)
            elif signal.modality == Modality.IMAGE:
                stored_files = self._store_image_files(signal, scenario_id)
            else:
                logger.error("Skip signal %s with Unsupported modality %s", signal.id, signal.modality)
        signal.files = stored_files

        signals = self._signals[scenario_id]
        if signal.id in signals:
            self._update(signals[signal.id], signal)
        else:
            self._controllers[scenario_id].append_signal(signal)
            signals[signal.id] = signal
            logger.debug("Added signal id to emissor file storage for scenario %s: %s", scenario_id, signal.id)

        self._is_modified[scenario_id] = True

    def _store_audio_files(self, audio_signal: AudioSignal, scenario_id: str):
        stored = []
        for url in audio_signal.files:
            dest_path, relative_path = self._destination_path(url, "wav", scenario_id)
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

    def _store_image_files(self, image_signal: ImageSignal, scenario_id: str):
        stored = []
        for url in image_signal.files:
            dest_path, relative_path = self._destination_path(url, "png", scenario_id)
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

    def _destination_path(self, url, postfix, scenario_id: str):
        relative_path = f"{url.replace('cltl-storage:', '')}.{postfix}"
        dest_path = os.path.join(self._storage.base_path, scenario_id, relative_path)

        os.makedirs(os.path.dirname(dest_path), exist_ok=True)

        return os.path.normpath(dest_path), relative_path

    def add_mention(self, mention: Mention):
        self._add_mention(mention)

    def add_mentions(self, mentions: Iterable[Mention]):
        for mention in mentions:
            self._add_mention(mention)

    def _add_mention(self, mention: Mention):
        container_id = mention.segment[0].container_id

        # Find the scenario that contains this signal
        scenario_id = None
        for sid, signals in self._signals.items():
            if container_id in signals:
                scenario_id = sid
                signal_id = container_id
                break
            elif container_id in self._signal_idx[sid]:
                scenario_id = sid
                signal_id = self._signal_idx[sid][container_id]
                break

        if not scenario_id:
            logger.warning(f"Container {container_id} not found in any active scenario for mention {mention.id}")
            return

        self._signals[scenario_id][signal_id].mentions.append(mention)
        self._signal_idx[scenario_id][mention.id] = signal_id
        logger.debug("Added mention id to emissor file storage for scenario %s: %s", scenario_id, mention.id)

        for annotation in mention.annotations:
            if isinstance(annotation, Container):
                self._signal_idx[scenario_id][annotation.id] = signal_id
                logger.debug("Added annotation id to emissor file storage for scenario %s: %s", scenario_id, annotation.id)
            elif isinstance(annotation.value, Container):
                self._signal_idx[scenario_id][annotation.value.id] = signal_id
                logger.debug("Added container id to emissor file storage for scenario %s: %s", scenario_id, annotation.value.id)

        self._is_modified[scenario_id] = True

    def _update(self, obj, update_obj):
        for key, value in vars(update_obj).items():
            if hasattr(obj, key) and value:
                setattr(obj, key, value)

    def get_signal(self, signal_id: str) -> Signal:
        # Search for signal across all scenarios
        for scenario_id, signals in self._signals.items():
            if signal_id in signals:
                return signals[signal_id]
        raise KeyError(f"Signal {signal_id} not found in any scenario")

    def get_scenario_for_id(self, element_id: str) -> str:
        # Search for element across all scenarios
        for scenario_id, signals in self._signals.items():
            if element_id in signals:
                return signals[element_id].time.container_id
            elif element_id in self._signal_idx[scenario_id]:
                signal_id = self._signal_idx[scenario_id][element_id]
                return signals[signal_id].time.container_id
        raise KeyError(f"Element {element_id} not found in any scenario")

    def flush(self):
        # Flush all modified scenarios
        for scenario_id in list(self._is_modified.keys()):
            if self._is_modified.get(scenario_id):
                self._flush_scenario(scenario_id)

    def _flush_scenario(self, scenario_id: str):
        if scenario_id in self._controllers and self._is_modified.get(scenario_id):
            self._storage.save_scenario(self._controllers[scenario_id])
            self._is_modified[scenario_id] = False
            logger.info("Persisted data for scenario %s", scenario_id)
