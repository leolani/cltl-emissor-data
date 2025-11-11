import dataclasses
import unittest
from tempfile import TemporaryDirectory

import numpy as np
import numpy.testing

from emissor.persistence import ScenarioStorage
from emissor.representation.annotation import Token
from emissor.representation.scenario import Scenario, ImageSignal, Modality, AudioSignal, Mention, Annotation

from cltl.emissordata.file_storage import EmissorDataFileStorage


@dataclasses.dataclass
class Face:
    """
    Information about a Face.

    Includes a vector representation of the face and optional meta information.
    """
    embedding: np.ndarray



class TestEmissorDataFileStorage(unittest.TestCase):
    def setUp(self) -> None:
        self.path = TemporaryDirectory(prefix=self.__class__.__name__)
        scenario_storage = ScenarioStorage(self.path.name)
        self.emissor_storage = EmissorDataFileStorage(
            self.path.name,
            audio_loader=None,
            image_loader=None,
            storage=scenario_storage
        )

    def tearDown(self) -> None:
        self.path.cleanup()

    def test_scenario_start(self):
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
        scenario = Scenario.new_instance("sc_1", 0, None, "", {"audio": "./audio"})
        self.emissor_storage.start_scenario(scenario)

        audio_signal = AudioSignal.for_scenario("sc_1", 0, None, "", -1, 2)
        self.emissor_storage.add_signal(audio_signal)
        self.emissor_storage.flush()

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
        scenario = Scenario.new_instance("sc_1", 0, None, "", {"audio": "./audio"})
        self.emissor_storage.start_scenario(scenario)
        start_signal = AudioSignal.for_scenario("sc_1", 0, None, "", -1, 2)
        self.emissor_storage.add_signal(start_signal)

        stop_signal = AudioSignal.for_scenario("sc_1", 0, 1, "", 1, 2, signal_id = start_signal.id)
        self.emissor_storage.add_signal(stop_signal)
        self.emissor_storage.flush()

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
        self.emissor_storage.flush()

        actual_signals = ScenarioStorage(self.path.name).load_modality("sc_1", Modality.AUDIO)
        self.assertEqual(2, len(actual_signals))
        self.assertEqual(audio_signal_1.id, actual_signals[0].id)
        self.assertEqual(audio_signal_2.id, actual_signals[1].id)
        self.assertEqual((0, 0, 1, 2), actual_signals[0].ruler.bounds)
        self.assertEqual((0, 0, 1, 2), actual_signals[1].ruler.bounds)

    def test_add_mention(self):
        scenario = Scenario.new_instance("sc_1", 0, None, "", {"audio": "./audio"})
        self.emissor_storage.start_scenario(scenario)
        audio_signal_1 = AudioSignal.for_scenario("sc_1", 0, 1, "", 1, 2)
        self.emissor_storage.add_signal(audio_signal_1)

        mention = Mention("men_1", [audio_signal_1.ruler.get_area_bounding_box(0, 0, 1, 1)],
                          [Annotation("test_annotation", "annotation", 1.0, 0)])
        self.emissor_storage.add_mention(mention)
        self.emissor_storage.flush()

        actual_signals = ScenarioStorage(self.path.name).load_modality("sc_1", Modality.AUDIO)
        self.assertEqual(1, len(actual_signals))
        self.assertEqual(audio_signal_1.id, actual_signals[0].id)
        self.assertEqual(1, len(actual_signals[0].mentions))
        self.assertEqual("men_1", actual_signals[0].mentions[0].id)

    def test_add_mention_on_annotation(self):
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
        self.emissor_storage.flush()

        actual_signals = ScenarioStorage(self.path.name).load_modality("sc_1", Modality.AUDIO)
        self.assertEqual(1, len(actual_signals))
        self.assertEqual(audio_signal_1.id, actual_signals[0].id)
        self.assertEqual(2, len(actual_signals[0].mentions))
        self.assertEqual("men_1", actual_signals[0].mentions[0].id)
        self.assertEqual("men_2", actual_signals[0].mentions[1].id)

    def test_add_mention_with_numpy_array(self):
        scenario = Scenario.new_instance("sc_1", 0, None, "", {"image": "./image"})
        self.emissor_storage.start_scenario(scenario)
        image_signal = ImageSignal.for_scenario("sc_1", 0, 1, "", (0, 0, 1, 1))
        self.emissor_storage.add_signal(image_signal)

        array = np.random.random((10, 10))
        mention = Mention("men_1", [image_signal.ruler.get_area_bounding_box(0, 0, 1, 1)],
                          [Annotation("test_annotation", Face(array), 1.0, 0)])
        self.emissor_storage.add_mention(mention)
        self.emissor_storage.flush()

        actual_signals = ScenarioStorage(self.path.name).load_modality("sc_1", Modality.IMAGE)
        self.assertEqual(1, len(actual_signals))
        self.assertEqual(image_signal.id, actual_signals[0].id)
        self.assertEqual(1, len(actual_signals[0].mentions))
        self.assertEqual("men_1", actual_signals[0].mentions[0].id)

        # The Face object with numpy array is serialized as a dict
        # When deserialized, the value is a dict with 'embedding' key
        actual_value = actual_signals[0].mentions[0].annotations[0].value
        if isinstance(actual_value, dict) and 'embedding' in actual_value:
            actual_embedding = np.array(actual_value['embedding'])
        else:
            actual_embedding = actual_value.embedding if hasattr(actual_value, 'embedding') else actual_value

        numpy.testing.assert_array_equal(array, actual_embedding)


import io
import json
import os
import zipfile
from tempfile import TemporaryDirectory

from cltl.emissordata.file_storage import EmissorDataFileStorage, DEFAULT_MAX_ZIP_SIZE_MB
from emissor.persistence import ScenarioStorage
from emissor.representation.scenario import Scenario, TextSignal, AudioSignal, ImageSignal


class TestFileStorageExtended(unittest.TestCase):
    """Extended tests for EmissorDataFileStorage storage-layer functionality."""

    def setUp(self) -> None:
        self.path = TemporaryDirectory(prefix=self.__class__.__name__)
        scenario_storage = ScenarioStorage(self.path.name)
        self.emissor_storage = EmissorDataFileStorage(
            self.path.name,
            audio_loader=None,
            image_loader=None,
            storage=scenario_storage
        )

    def tearDown(self) -> None:
        self.path.cleanup()

    def _create_test_scenario(self, scenario_id: str, include_signals: bool = True):
        """Helper method to create a test scenario with optional signals."""
        signals = {"text": "./text", "audio": "./audio", "image": "./image"} if include_signals else {}
        scenario = Scenario.new_instance(scenario_id, 0, 1000, "test context", signals)
        self.emissor_storage.start_scenario(scenario)

        if include_signals:
            # Add text signal
            text_signal = TextSignal.for_scenario(scenario_id, 0, 100, "Hello world")
            self.emissor_storage.add_signal(text_signal)

            # Add audio signal (without actual files)
            audio_signal = AudioSignal.for_scenario(scenario_id, 100, 200, "", -1, 2)
            self.emissor_storage.add_signal(audio_signal)

            # Add image signal (without actual files)
            image_signal = ImageSignal.for_scenario(scenario_id, 200, 300, "", (0, 0, 100, 100))
            self.emissor_storage.add_signal(image_signal)

        self.emissor_storage.stop_scenario(scenario)
        return scenario


class TestListScenarios(TestFileStorageExtended):
    """Tests for list_scenarios() method."""

    def test_list_scenarios_empty(self):
        """Test listing scenarios when none exist."""
        scenarios = self.emissor_storage.list_scenarios()
        self.assertEqual([], scenarios)

    def test_list_scenarios_single(self):
        """Test listing scenarios with one scenario."""
        scenario = self._create_test_scenario("test_scenario_1")

        scenarios = self.emissor_storage.list_scenarios()

        self.assertEqual(1, len(scenarios))
        scenario_data = scenarios[0]
        self.assertEqual("test_scenario_1", scenario_data['id'])
        self.assertEqual(0, scenario_data['start'])
        self.assertEqual(1000, scenario_data['end'])
        self.assertIn('context', scenario_data)

    def test_list_scenarios_multiple(self):
        """Test listing multiple scenarios."""
        self._create_test_scenario("scenario_1")
        self._create_test_scenario("scenario_2")
        self._create_test_scenario("scenario_3")

        scenarios = self.emissor_storage.list_scenarios()

        self.assertEqual(3, len(scenarios))
        scenario_ids = {s['id'] for s in scenarios}
        self.assertEqual({"scenario_1", "scenario_2", "scenario_3"}, scenario_ids)

    def test_list_scenarios_with_corrupted_scenario(self):
        """Test that corrupted scenarios are skipped gracefully."""
        # Create valid scenario
        self._create_test_scenario("valid_scenario")

        # Create corrupted scenario by creating directory but invalid data
        corrupted_path = os.path.join(self.path.name, "corrupted_scenario")
        os.makedirs(corrupted_path, exist_ok=True)

        # Write invalid JSON to scenario file
        scenario_file = os.path.join(corrupted_path, "corrupted_scenario.json")
        with open(scenario_file, 'w') as f:
            f.write("invalid json content")

        scenarios = self.emissor_storage.list_scenarios()

        # Should succeed but only return valid scenario
        self.assertEqual(1, len(scenarios))
        self.assertEqual("valid_scenario", scenarios[0]['id'])


class TestGetScenario(TestFileStorageExtended):
    """Tests for get_scenario() method."""

    def test_get_scenario_success(self):
        """Test getting a scenario with all signals."""
        scenario = self._create_test_scenario("test_scenario")

        scenario_data = self.emissor_storage.get_scenario("test_scenario")

        self.assertIn('scenario', scenario_data)
        self.assertIn('signals', scenario_data)

        # Verify scenario data exists (it's marshalled JSON)
        self.assertIsNotNone(scenario_data['scenario'])

        # Verify all modalities are present
        signals = scenario_data['signals']
        self.assertIn('text', signals)
        self.assertIn('audio', signals)
        self.assertIn('image', signals)
        self.assertEqual(1, len(signals['text']))
        self.assertEqual(1, len(signals['audio']))
        self.assertEqual(1, len(signals['image']))

    def test_get_scenario_not_found(self):
        """Test getting non-existent scenario returns error."""
        with self.assertRaises(ValueError) as context:
            self.emissor_storage.get_scenario("nonexistent_scenario")

        # Error message contains the scenario ID
        self.assertIn("nonexistent_scenario", str(context.exception).lower())

    def test_get_scenario_no_signals(self):
        """Test getting scenario with no signals."""
        self._create_test_scenario("empty_scenario", include_signals=False)

        scenario_data = self.emissor_storage.get_scenario("empty_scenario")

        self.assertIn('scenario', scenario_data)
        self.assertIn('signals', scenario_data)
        self.assertEqual({'audio': [], 'image': [], 'text': []}, scenario_data['signals'])

    def test_get_scenario_partial_modalities(self):
        """Test scenario with only some modalities."""
        scenario_id = "partial_scenario"
        scenario = Scenario.new_instance(scenario_id, 0, 1000, "test", {"text": "./text"})
        self.emissor_storage.start_scenario(scenario)

        # Only add text signal
        text_signal = TextSignal.for_scenario(scenario_id, 0, 100, "Only text")
        self.emissor_storage.add_signal(text_signal)

        self.emissor_storage.stop_scenario(scenario)

        scenario_data = self.emissor_storage.get_scenario(scenario_id)

        signals = scenario_data['signals']
        self.assertIn('text', signals)
        self.assertEqual(1, len(signals['text']))
        # Audio and image should be present with empty arrays
        self.assertIn('audio', signals)
        self.assertIn('image', signals)
        self.assertEqual(0, len(signals['audio']))
        self.assertEqual(0, len(signals['image']))

    def test_get_scenario_with_missing_signal_files(self):
        """Test scenario with signals but missing actual files is handled gracefully."""
        scenario_id = "missing_files_scenario"
        scenario = Scenario.new_instance(scenario_id, 0, 1000, "test", {"audio": "./audio"})
        self.emissor_storage.start_scenario(scenario)

        # Add signals that reference files that don't exist
        audio_signal = AudioSignal.for_scenario(scenario_id, 0, 100, "", -1, 2)
        audio_signal.files = ["nonexistent.wav"]
        self.emissor_storage.add_signal(audio_signal)

        self.emissor_storage.stop_scenario(scenario)

        # Should still return scenario even if signal files are missing
        scenario_data = self.emissor_storage.get_scenario(scenario_id)

        self.assertIn('scenario', scenario_data)
        self.assertIn('signals', scenario_data)


class TestCreateScenarioZip(TestFileStorageExtended):
    """Tests for create_scenario_zip() method."""

    def test_create_scenario_zip_success(self):
        """Test successful scenario zip creation."""
        self._create_test_scenario("test_scenario")

        zip_buffer = self.emissor_storage.create_scenario_zip("test_scenario")

        self.assertIsInstance(zip_buffer, io.BytesIO)
        # Buffer should have content (check size, not position)
        self.assertGreater(len(zip_buffer.getvalue()), 0)

        # Verify zip file can be read
        with zipfile.ZipFile(zip_buffer, 'r') as zf:
            file_list = zf.namelist()
            self.assertTrue(any('test_scenario' in f for f in file_list))

    def test_create_scenario_zip_not_found(self):
        """Test zip creation for non-existent scenario."""
        with self.assertRaises(ValueError) as context:
            self.emissor_storage.create_scenario_zip("nonexistent")

        self.assertIn("not found", str(context.exception).lower())

    def test_create_scenario_zip_path_traversal_blocked(self):
        """Test that path traversal attempts are blocked."""
        path_traversal_ids = [
            "../etc/passwd",
            "../../secret",
            "test/../../../etc/passwd"
        ]

        for scenario_id in path_traversal_ids:
            with self.assertRaises(ValueError) as context:
                self.emissor_storage.create_scenario_zip(scenario_id)

            self.assertIn("traversal", str(context.exception).lower())

    def test_create_scenario_zip_too_large(self):
        """Test that scenarios exceeding size limit are rejected."""
        # Create scenario
        scenario_id = "large_scenario"
        self._create_test_scenario(scenario_id)

        # Create large files in scenario directory
        scenario_path = os.path.join(self.path.name, scenario_id)
        large_file = os.path.join(scenario_path, "large_file.bin")

        # Create file larger than limit
        with open(large_file, 'wb') as f:
            f.write(b'0' * ((DEFAULT_MAX_ZIP_SIZE_MB + 1) * 1024 * 1024))

        with self.assertRaises(ValueError) as context:
            self.emissor_storage.create_scenario_zip(scenario_id)

        self.assertIn("too large", str(context.exception).lower())

    def test_create_scenario_zip_empty(self):
        """Test zip creation with minimal content."""
        self._create_test_scenario("minimal_scenario", include_signals=False)

        zip_buffer = self.emissor_storage.create_scenario_zip("minimal_scenario")

        self.assertIsInstance(zip_buffer, io.BytesIO)
        # Buffer should have content (check size, not position)
        self.assertGreater(len(zip_buffer.getvalue()), 0)


class TestScenarioPathTraversalSecurity(TestFileStorageExtended):
    """Security tests for path traversal prevention."""

    def test_create_scenario_zip_path_traversal_blocked(self):
        """Test that _create_scenario_zip blocks path traversal."""
        malicious_ids = [
            "../",
            "../../etc/passwd",
            "test/../../secrets",
            "..",
            "../../../"
        ]

        for scenario_id in malicious_ids:
            with self.assertRaises(ValueError):
                self.emissor_storage.create_scenario_zip(scenario_id)

    def test_normalized_paths_prevent_traversal(self):
        """Test that path normalization prevents directory traversal."""
        # Try to access parent directory
        with self.assertRaises(ValueError):
            self.emissor_storage.create_scenario_zip("valid/../..")

    def test_scenario_zip_validates_directory_exists(self):
        """Test that zip creation validates directory exists and is a directory."""
        # Non-existent directory
        with self.assertRaises(ValueError):
            self.emissor_storage.create_scenario_zip("nonexistent_dir")

        # Create a file instead of directory
        file_path = os.path.join(self.path.name, "file_not_dir")
        with open(file_path, 'w') as f:
            f.write("content")

        with self.assertRaises(ValueError):
            self.emissor_storage.create_scenario_zip("file_not_dir")


class TestGetStoragePath(TestFileStorageExtended):
    """Tests for get_storage_path() method."""

    def test_get_storage_path(self):
        """Test that storage path is returned correctly."""
        path = self.emissor_storage.get_storage_path()
        self.assertEqual(self.path.name, path)
