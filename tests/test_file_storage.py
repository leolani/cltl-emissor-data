import os
import unittest
from tempfile import TemporaryDirectory

from emissor.persistence import ScenarioStorage
from emissor.representation.annotation import Token
from emissor.representation.scenario import Scenario, ImageSignal, Modality, AudioSignal, Mention, Annotation

from cltl.emissordata.file_storage import EmissorDataFileStorage


class TestEmissorDataFileStorage(unittest.TestCase):
    def setUp(self) -> None:
        self.path = TemporaryDirectory(prefix=self.__class__.__name__)
        self.emissor_storage = EmissorDataFileStorage(self.path.name)

    def tearDown(self) -> None:
        self.path.cleanup()

    def test_scenario_start(self):
        os.makedirs(self.path.name + "/sc_1")
        scenario_start = Scenario.new_instance("sc_1", 0, None, "", {"image": "./image"})

        self.emissor_storage.start_scenario(scenario_start)

        actual = ScenarioStorage(self.path.name).load_scenario("sc_1")
        self.assertEqual("sc_1", actual.scenario.id)
        self.assertEqual("sc_1", actual.scenario.ruler.container_id)
        self.assertEqual(0, actual.scenario.ruler.start)
        self.assertEqual(None, actual.scenario.ruler.end)
        self.assertEqual("", actual.scenario.context)
        self.assertEqual({"image": "./image"}, actual.scenario.signals)

    def test_scenario_stop(self):
        os.makedirs(self.path.name + "/sc_1")

        scenario_start = Scenario.new_instance("sc_1", 0, None, "", {"image": "./image"})
        self.emissor_storage.start_scenario(scenario_start)
        scenario_stop = Scenario.new_instance("sc_1", 0, 1, "", {"image": "./image"})
        self.emissor_storage.stop_scenario(scenario_stop)

        actual = ScenarioStorage(self.path.name).load_scenario("sc_1")
        self.assertEqual("sc_1", actual.scenario.id)
        self.assertEqual("sc_1", actual.scenario.ruler.container_id)
        self.assertEqual(0, actual.scenario.ruler.start)
        self.assertEqual(1, actual.scenario.ruler.end)
        self.assertEqual("", actual.scenario.context)
        self.assertEqual({"image": "./image"}, actual.scenario.signals)

    def test_signal_start(self):
        os.makedirs(self.path.name + "/sc_1")

        scenario = Scenario.new_instance("sc_1", 0, None, "", {"audio": "./audio"})
        self.emissor_storage.start_scenario(scenario)

        audio_signal = AudioSignal.for_scenario("sc_1", 0, None, "", -1, 2)
        self.emissor_storage.add_signal(audio_signal)

        actual = ScenarioStorage(self.path.name).load_scenario("sc_1")
        self.assertEqual(scenario, actual.scenario)
        actual_signals = ScenarioStorage(self.path.name).load_modality("sc_1", Modality.AUDIO)
        self.assertEqual(1, len(actual_signals))
        self.assertEqual(audio_signal.id, actual_signals[0].id)
        self.assertEqual(0, actual_signals[0].time.start)
        self.assertEqual(None, actual_signals[0].time.end)
        self.assertEqual("sc_1", actual_signals[0].time.container_id)
        self.assertEqual((0, 0, -1, 2), actual_signals[0].ruler.bounds)

    def test_signal_stop(self):
        os.makedirs(self.path.name + "/sc_1")

        scenario = Scenario.new_instance("sc_1", 0, None, "", {"audio": "./audio"})
        self.emissor_storage.start_scenario(scenario)
        start_signal = AudioSignal.for_scenario("sc_1", 0, None, "", -1, 2)
        self.emissor_storage.add_signal(start_signal)

        stop_signal = AudioSignal.for_scenario("sc_1", 0, 1, "", 1, 2, signal_id = start_signal.id)
        self.emissor_storage.add_signal(stop_signal)

        actual = ScenarioStorage(self.path.name).load_scenario("sc_1")
        self.assertEqual(scenario, actual.scenario)
        actual_signals = ScenarioStorage(self.path.name).load_modality("sc_1", Modality.AUDIO)
        self.assertEqual(1, len(actual_signals))
        self.assertEqual(start_signal.id, actual_signals[0].id)
        self.assertEqual(0, actual_signals[0].time.start)
        self.assertEqual(1, actual_signals[0].time.end)
        self.assertEqual("sc_1", actual_signals[0].time.container_id)
        self.assertEqual((0, 0, 1, 2), actual_signals[0].ruler.bounds)

    def test_multiple_signals(self):
        os.makedirs(self.path.name + "/sc_1")

        scenario = Scenario.new_instance("sc_1", 0, None, "", {"audio": "./audio"})
        self.emissor_storage.start_scenario(scenario)

        audio_signal_1 = AudioSignal.for_scenario("sc_1", 0, None, "", -1, 2)
        self.emissor_storage.add_signal(audio_signal_1)
        stop_signal = AudioSignal.for_scenario("sc_1", 0, 1, "", 1, 2, signal_id=audio_signal_1.id)
        self.emissor_storage.add_signal(stop_signal)

        audio_signal_2 = AudioSignal.for_scenario("sc_1", 0, None, "", -1, 2)
        self.emissor_storage.add_signal(audio_signal_2)
        stop_signal = AudioSignal.for_scenario("sc_1", 0, 1, "", 1, 2, signal_id=audio_signal_2.id)
        self.emissor_storage.add_signal(stop_signal)

        actual_signals = ScenarioStorage(self.path.name).load_modality("sc_1", Modality.AUDIO)
        self.assertEqual(2, len(actual_signals))
        self.assertEqual(audio_signal_1.id, actual_signals[0].id)
        self.assertEqual(audio_signal_2.id, actual_signals[1].id)
        self.assertEqual((0, 0, 1, 2), actual_signals[0].ruler.bounds)
        self.assertEqual((0, 0, 1, 2), actual_signals[1].ruler.bounds)

    def test_add_mention(self):
        os.makedirs(self.path.name + "/sc_1")

        scenario = Scenario.new_instance("sc_1", 0, None, "", {"audio": "./audio"})
        self.emissor_storage.start_scenario(scenario)
        audio_signal_1 = AudioSignal.for_scenario("sc_1", 0, 1, "", 1, 2)
        self.emissor_storage.add_signal(audio_signal_1)

        mention = Mention("men_1", [audio_signal_1.ruler.get_area_bounding_box(0, 0, 1, 1)],
                          [Annotation("test_annotation", "annotation", 1.0, 0)])
        self.emissor_storage.add_mention(mention)

        actual_signals = ScenarioStorage(self.path.name).load_modality("sc_1", Modality.AUDIO)
        self.assertEqual(1, len(actual_signals))
        self.assertEqual(audio_signal_1.id, actual_signals[0].id)
        self.assertEqual(1, len(actual_signals[0].mentions))
        self.assertEqual("men_1", actual_signals[0].mentions[0].id)

    def test_add_mention_on_annotation(self):
        os.makedirs(self.path.name + "/sc_1")

        scenario = Scenario.new_instance("sc_1", 0, None, "", {"audio": "./audio"})
        self.emissor_storage.start_scenario(scenario)
        audio_signal_1 = AudioSignal.for_scenario("sc_1", 0, 1, "", 1, 2)
        self.emissor_storage.add_signal(audio_signal_1)

        token_annotation = Token.for_string("test_annotation")
        mention = Mention("men_1", [audio_signal_1.ruler.get_area_bounding_box(0, 0, 1, 1)], [token_annotation])
        self.emissor_storage.add_mention(mention)

        test_annotation = Annotation("test_annotation", "annotation", 1.0, 0)
        mention = Mention("men_2", [token_annotation.ruler], [test_annotation])
        self.emissor_storage.add_mention(mention)

        actual_signals = ScenarioStorage(self.path.name).load_modality("sc_1", Modality.AUDIO)
        self.assertEqual(1, len(actual_signals))
        self.assertEqual(audio_signal_1.id, actual_signals[0].id)
        self.assertEqual(2, len(actual_signals[0].mentions))
        self.assertEqual("men_1", actual_signals[0].mentions[0].id)
        self.assertEqual("men_2", actual_signals[0].mentions[1].id)