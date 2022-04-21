import logging
from typing import List

from cltl.combot.event.emissor import ScenarioStarted, ScenarioStopped, SignalEvent, AnnotationEvent
from cltl.combot.infra.config import ConfigurationManager
from cltl.combot.infra.event import Event, EventBus
from cltl.combot.infra.resource import ResourceManager
from cltl.combot.infra.topic_worker import TopicWorker
from flask import Flask, Response

from cltl.emissordata.api import EmissorDataStorage

logger = logging.getLogger(__name__)


class EmissorDataService:
    @classmethod
    def from_config(cls, storage: EmissorDataStorage, event_bus: EventBus, resource_manager: ResourceManager,
                    config_manager: ConfigurationManager):
        config = config_manager.get_config("cltl.emissor-data.event")

        return cls(config.get("topics", multi=True), storage, event_bus, resource_manager)

    def __init__(self, input_topics: List[str], storage: EmissorDataStorage,
                 event_bus: EventBus, resource_manager: ResourceManager):
        self._storage = storage

        self._event_bus = event_bus
        self._resource_manager = resource_manager

        self._input_topics = input_topics

        self._topic_worker = None
        self._app = None

    def start(self, timeout=30):
        self._topic_worker = TopicWorker(self._input_topics, self._event_bus,
                                         resource_manager=self._resource_manager,
                                         processor=self._process,
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

        @self._app.route(f"/emissor/<paramter>", methods=['GET'])
        def store_audio(parameter: str):
            return Response(status=404)

        @self._app.after_request
        def set_cache_control(response):
            response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
            response.headers['Pragma'] = 'no-cache'
            response.headers['Expires'] = '0'

            return response

        return self._app

    def _process(self, event: Event):
        if event.payload.type == ScenarioStarted.__name__:
            self._storage.start_scenario(event.payload.scenario)
            logger.debug("Received scenario started event for scenario %s", event.payload.scenario.id)
        if event.payload.type == ScenarioStopped.__name__:
            self._storage.stop_scenario(event.payload.scenario)
            logger.debug("Received scenario stopped event for scenario %s", event.payload.scenario.id)
        if hasattr(event.payload, 'signal'):
            self._storage.add_signal(event.payload.signal)
            logger.debug("Received signal event %s", event.payload.signal.id)
        if hasattr(event.payload, 'mentions'):
            self._storage.add_mentions(event.payload.mentions)
            logger.debug("Received mentions event %s" + event.payload.type)
