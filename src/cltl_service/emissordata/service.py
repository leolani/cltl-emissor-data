import io
import logging
import os
import time
import zipfile
from typing import List

from cltl.combot.event.emissor import ScenarioStarted, ScenarioStopped, ScenarioEvent
from cltl.combot.infra.config import ConfigurationManager
from cltl.combot.infra.event import Event, EventBus
from cltl.combot.infra.resource import ResourceManager
from cltl.combot.infra.topic_worker import TopicWorker, RejectionStrategy
from flask import Flask, jsonify, send_file

from cltl.emissordata.api import EmissorDataStorage
from emissor.representation.scenario import Modality

logger = logging.getLogger(__name__)

MAX_SCENARIO_ID_LENGTH = 255
MAX_ZIP_SIZE_MB = 500


class EmissorDataService:
    @classmethod
    def from_config(cls, storage: EmissorDataStorage, event_bus: EventBus, resource_manager: ResourceManager,
                    config_manager: ConfigurationManager):
        config = config_manager.get_config("cltl.emissor-data")
        flush_interval = config.get_int("flush_interval") if "flush_interval" in config else -1

        config = config_manager.get_config("cltl.emissor-data.event")
        event_topics = config.get("topics", multi=True)

        return cls(event_topics, flush_interval, storage,
                   event_bus, resource_manager)

    def __init__(self, input_topics: List[str], flush_interval: int, storage: EmissorDataStorage,
                 event_bus: EventBus, resource_manager: ResourceManager):
        self._storage = storage
        self._flush_interval = flush_interval
        self._last_flush = time.time()

        self._event_bus = event_bus
        self._resource_manager = resource_manager

        self._input_topics = input_topics

        self._topic_worker = None
        self._app = None

    def start(self, timeout=30):
        self._topic_worker = TopicWorker(self._input_topics, self._event_bus,
                                         buffer_size=2048, rejection_strategy=RejectionStrategy.EXCEPTION,
                                         resource_manager=self._resource_manager,
                                         processor=self._process,
                                         scheduled=self._flush_interval if self._flush_interval > 0 else None,
                                         name=self.__class__.__name__)
        self._topic_worker.start().wait()

    def stop(self):
        if not self._topic_worker:
            pass

        self._topic_worker.stop()
        self._topic_worker.await_stop()
        self._topic_worker = None

    @property
    def app(self):
        """
        Flask endpoint for REST interface.
        """
        if self._app:
            return self._app

        storage_path = self._get_storage_path()
        self._app = Flask("emissordata", static_folder=storage_path, static_url_path='/storage')

        @self._app.route(f"/<element_id>/scenario/id", methods=['GET'])
        def get_scenario_id(element_id: str):
            try:
                return self._storage.get_scenario_for_id(element_id), 200
            except KeyError:
                return "", 404

        @self._app.route("/scenarios", methods=['GET'])
        def list_scenarios():
            try:
                scenarios = self._list_scenarios()
                return jsonify({"scenarios": scenarios}), 200
            except Exception as e:
                logger.exception("Error listing scenarios")
                return jsonify({"error": "Internal server error"}), 500

        @self._app.route("/scenarios/<scenario_id>", methods=['GET'])
        def get_scenario(scenario_id: str):
            try:
                self._validate_scenario_id(scenario_id)
                scenario_data = self._get_scenario(scenario_id)
                return jsonify(scenario_data), 200
            except ValueError as e:
                logger.error("Invalid request for scenario %s: %s", scenario_id, e)
                return jsonify({"error": "Scenario not found"}), 404
            except Exception as e:
                logger.exception("Error getting scenario %s", scenario_id)
                return jsonify({"error": "Internal server error"}), 500

        @self._app.route("/scenarios/<scenario_id>/download", methods=['GET'])
        def download_scenario(scenario_id: str):
            try:
                self._validate_scenario_id(scenario_id)
                zip_buffer = self._create_scenario_zip(scenario_id)
                return send_file(
                    zip_buffer,
                    mimetype='application/zip',
                    as_attachment=True,
                    attachment_filename=f'{scenario_id}.zip'
                ), 200
            except ValueError as e:
                logger.error("Invalid request for scenario download %s: %s", scenario_id, e)
                return jsonify({"error": "Scenario not found"}), 404
            except Exception as e:
                logger.exception("Error creating scenario zip for %s", scenario_id)
                return jsonify({"error": "Internal server error"}), 500

        @self._app.after_request
        def set_cache_control(response):
            response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
            response.headers['Pragma'] = 'no-cache'
            response.headers['Expires'] = '0'

            return response

        return self._app

    def _process(self, event: Event):
        if not event:
            # Scheduled invocation
            pass
        elif event.payload.type == ScenarioStarted.__name__:
            self._storage.start_scenario(event.payload.scenario)
            logger.debug("Received scenario started event for scenario %s", event.payload.scenario.id)
        elif event.payload.type == ScenarioStopped.__name__:
            self._storage.stop_scenario(event.payload.scenario)
            logger.debug("Received scenario stopped event for scenario %s", event.payload.scenario.id)
        elif event.payload.type == ScenarioEvent.__name__:
            self._storage.update_scenario(event.payload.scenario)
            logger.debug("Received scenario event for scenario %s", event.payload.scenario.id)
        elif hasattr(event.payload, 'signal'):
            self._storage.add_signal(event.payload.signal)
            logger.debug("Received signal event %s on topic %s", event.payload.signal.id, event.metadata.topic)
        elif hasattr(event.payload, 'mentions'):
            self._storage.add_mentions(event.payload.mentions)
            logger.debug("Received mentions event %s on topic %s", event.payload.type, event.metadata.topic)

        if (self._flush_interval > -1
                and (not event
                     or self._flush_interval == 0
                     or (time.time() - self._last_flush) > self._flush_interval)):
            self._storage.flush()
            self._last_flush = time.time()

    def _validate_scenario_id(self, scenario_id: str) -> None:
        if not scenario_id or len(scenario_id) > MAX_SCENARIO_ID_LENGTH:
            raise ValueError("Invalid scenario_id length")

        if not all(c.isalnum() or c in '-_' for c in scenario_id):
            raise ValueError("Invalid scenario_id format: only alphanumeric, dash, and underscore allowed")

    def _get_storage_path(self):
        from cltl.emissordata.file_storage import EmissorDataFileStorage
        if isinstance(self._storage, EmissorDataFileStorage):
            return self._storage._storage.base_path
        return None

    def _list_scenarios(self):
        from cltl.emissordata.file_storage import EmissorDataFileStorage
        from emissor.representation.util import marshal

        if not isinstance(self._storage, EmissorDataFileStorage):
            raise ValueError("Storage implementation does not support listing scenarios")

        storage = self._storage._storage
        scenario_ids = storage.list_scenarios()
        scenarios = []

        for scenario_id in scenario_ids:
            try:
                controller = storage.load_scenario(scenario_id)
                if controller:
                    scenario = controller.scenario
                    scenarios.append({
                        "id": scenario.id,
                        "start": scenario.start,
                        "end": scenario.end,
                        "context": marshal(scenario.context)
                    })
            except Exception as e:
                logger.warning("Failed to load scenario %s: %s", scenario_id, e)

        return scenarios

    def _get_scenario(self, scenario_id: str):
        from cltl.emissordata.file_storage import EmissorDataFileStorage
        from emissor.representation.util import marshal

        if not isinstance(self._storage, EmissorDataFileStorage):
            raise ValueError("Storage implementation does not support getting scenarios")

        storage = self._storage._storage
        controller = storage.load_scenario(scenario_id)

        if not controller:
            raise ValueError(f"Scenario {scenario_id} not found")

        try:
            controller.load_signals((Modality.TEXT, Modality.AUDIO, Modality.IMAGE))
        except Exception as e:
            logger.warning("Failed to load some signals for scenario %s: %s", scenario_id, e)

        scenario_data = {
            "scenario": marshal(controller.scenario),
            "signals": {}
        }

        for modality, signals in controller.signals.items():
            scenario_data["signals"][modality.name.lower()] = [marshal(signal) for signal in signals]

        return scenario_data

    def _create_scenario_zip(self, scenario_id: str):
        from cltl.emissordata.file_storage import EmissorDataFileStorage

        if not isinstance(self._storage, EmissorDataFileStorage):
            raise ValueError("Storage implementation does not support zip download")

        storage = self._storage._storage
        base_path_normalized = os.path.normpath(storage.base_path)
        scenario_path = os.path.normpath(os.path.join(base_path_normalized, scenario_id))

        if not scenario_path.startswith(base_path_normalized + os.sep):
            raise ValueError("Invalid scenario path: path traversal detected")

        if not os.path.exists(scenario_path):
            raise ValueError(f"Scenario {scenario_id} not found")

        if not os.path.isdir(scenario_path):
            raise ValueError(f"Scenario {scenario_id} is not a directory")

        total_size = sum(
            os.path.getsize(os.path.join(root, file))
            for root, dirs, files in os.walk(scenario_path)
            for file in files
        )

        if total_size > MAX_ZIP_SIZE_MB * 1024 * 1024:
            raise ValueError(
                f"Scenario too large to download: {total_size / (1024 * 1024):.1f}MB "
                f"(max: {MAX_ZIP_SIZE_MB}MB)"
            )

        zip_buffer = io.BytesIO()

        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for root, dirs, files in os.walk(scenario_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, storage.base_path)
                    zip_file.write(file_path, arcname)

        zip_buffer.seek(0)
        return zip_buffer