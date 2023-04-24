import logging
import time
from typing import List

from cltl.combot.event.emissor import ScenarioStarted, ScenarioStopped, ScenarioEvent
from cltl.combot.infra.config import ConfigurationManager
from cltl.combot.infra.event import Event, EventBus
from cltl.combot.infra.resource import ResourceManager
from cltl.combot.infra.topic_worker import TopicWorker, RejectionStrategy
from flask import Flask

from cltl.emissordata.api import EmissorDataStorage

logger = logging.getLogger(__name__)


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

        self._app = Flask("emissordata")

        @self._app.route(f"/<element_id>/scenario/id", methods=['GET'])
        def get_scenario_id(element_id: str):
            try:
                return self._storage.get_scenario_for_id(element_id), 200
            except KeyError:
                return self._storage.get_current_scenario_id(), 404

        @self._app.route(f"/scenario/current/id", methods=['GET'])
        def get_current_scenario():
            scensario_id = self._storage.get_current_scensario_id()
            return scensario_id, 200 if scensario_id else "", 404

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