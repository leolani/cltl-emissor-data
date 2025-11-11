import json
import unittest
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

from cltl.emissordata.file_storage import EmissorDataFileStorage
from cltl_service.emissordata.service import EmissorDataService, MAX_SCENARIO_ID_LENGTH
from emissor.persistence import ScenarioStorage
from emissor.representation.scenario import Scenario, TextSignal, AudioSignal, ImageSignal


class TestEmissorDataServiceEndpoints(unittest.TestCase):
    """Test suite for EmissorDataService HTTP endpoints."""

    def setUp(self):
        """Set up test fixtures before each test."""
        self.temp_dir = TemporaryDirectory(prefix=self.__class__.__name__)
        self.storage_path = self.temp_dir.name

        # Create storage and service
        scenario_storage = ScenarioStorage(self.storage_path)
        self.emissor_storage = EmissorDataFileStorage(
            self.storage_path,
            audio_loader=None,
            image_loader=None,
            storage=scenario_storage
        )

        # Mock event bus and resource manager
        self.event_bus = Mock()
        self.resource_manager = Mock()

        # Create service
        self.service = EmissorDataService(
            input_topics=[],
            flush_interval=-1,
            storage=self.emissor_storage,
            event_bus=self.event_bus,
            resource_manager=self.resource_manager
        )

        # Get Flask app for testing
        self.app = self.service.app
        self.client = self.app.test_client()

    def tearDown(self):
        """Clean up after each test."""
        self.temp_dir.cleanup()

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


class TestListScenariosEndpoint(TestEmissorDataServiceEndpoints):
    """Tests for GET /scenarios endpoint - HTTP layer only."""

    def test_list_scenarios_internal_error_handling(self):
        """Test that internal errors return 500."""
        # Mock the storage to raise an exception
        with patch.object(self.service, '_list_scenarios', side_effect=RuntimeError("Test error")):
            response = self.client.get('/scenarios')

            self.assertEqual(500, response.status_code)
            data = json.loads(response.data)
            self.assertIn('error', data)


class TestGetScenarioEndpoint(TestEmissorDataServiceEndpoints):
    """Tests for GET /scenarios/<id> endpoint - HTTP layer only."""

    def test_get_scenario_invalid_id_format(self):
        """Test that invalid scenario_id format is rejected."""
        invalid_ids = [
            "scenario@test",
            "scenario!invalid",
            "scenario with spaces",
            "scenario\\backslash",
            "scenario;command",
            "scenario|pipe"
        ]

        for scenario_id in invalid_ids:
            response = self.client.get(f'/scenarios/{scenario_id}')

            # Should return 400 for invalid format
            self.assertEqual(400, response.status_code)
            data = json.loads(response.data)
            self.assertIn('error', data)

    def test_get_scenario_id_too_long(self):
        """Test that scenario_id exceeding max length is rejected."""
        long_id = "a" * (MAX_SCENARIO_ID_LENGTH + 1)

        response = self.client.get(f'/scenarios/{long_id}')

        self.assertEqual(400, response.status_code)
        data = json.loads(response.data)
        self.assertIn('error', data)

    def test_get_scenario_internal_error(self):
        """Test that internal errors return 500."""
        # Create a scenario first
        self._create_test_scenario("test_scenario")

        # Mock the storage to raise an exception
        with patch.object(self.service, '_get_scenario', side_effect=RuntimeError("Test error")):
            response = self.client.get('/scenarios/test_scenario')

            self.assertEqual(500, response.status_code)
            data = json.loads(response.data)
            self.assertIn('error', data)
            self.assertEqual("Internal server error", data['error'])


class TestDownloadScenarioEndpoint(TestEmissorDataServiceEndpoints):
    """Tests for GET /scenarios/<id>/download endpoint - HTTP layer only."""

    def test_download_scenario_invalid_id_format(self):
        """Test that invalid scenario_id formats are rejected."""
        invalid_ids = [
            "scenario@test",
            "scenario!invalid",
            "scenario with spaces",
            "scenario\\backslash",
            "scenario;command",
            "scenario|pipe"
        ]

        for scenario_id in invalid_ids:
            response = self.client.get(f'/scenarios/{scenario_id}/download')

            # Should return 400 for invalid format
            self.assertEqual(400, response.status_code)
            data = json.loads(response.data)
            self.assertIn('error', data)

    def test_download_scenario_internal_error(self):
        """Test that internal errors return 500."""
        # Create a scenario first
        self._create_test_scenario("test_scenario")

        # Mock the zip creation to raise an exception
        with patch.object(self.service, '_create_scenario_zip', side_effect=RuntimeError("Test error")):
            response = self.client.get('/scenarios/test_scenario/download')

            self.assertEqual(500, response.status_code)
            data = json.loads(response.data)
            self.assertIn('error', data)


class TestValidateScenarioId(TestEmissorDataServiceEndpoints):
    """Tests for scenario ID validation."""

    def test_validate_valid_ids(self):
        """Test that valid scenario IDs pass validation."""
        valid_ids = [
            "scenario-1",
            "test_scenario",
            "MyScenario123",
            "a",
            "123",
            "test-scenario_123"
        ]

        for scenario_id in valid_ids:
            # Should not raise
            self.service._validate_scenario_id(scenario_id)

    def test_validate_empty_id(self):
        """Test that empty scenario_id is rejected."""
        with self.assertRaises(ValueError):
            self.service._validate_scenario_id("")

    def test_validate_none_id(self):
        """Test that None scenario_id is rejected."""
        with self.assertRaises(ValueError):
            self.service._validate_scenario_id(None)

    def test_validate_id_too_long(self):
        """Test that scenario_id exceeding max length is rejected."""
        long_id = "a" * (MAX_SCENARIO_ID_LENGTH + 1)

        with self.assertRaises(ValueError) as context:
            self.service._validate_scenario_id(long_id)

        self.assertIn("length", str(context.exception).lower())

    def test_validate_id_with_special_characters(self):
        """Test that scenario_ids with special characters are rejected."""
        invalid_ids = [
            "scenario@test",
            "scenario!invalid",
            "scenario with spaces",
            "scenario/slash",
            "scenario\\backslash",
            "scenario;command",
            "scenario|pipe",
            "scenario&ampersand",
            "scenario$dollar",
            "scenario%percent",
            "scenario^caret",
            "scenario*asterisk",
            "scenario(paren",
            "scenario)paren",
            "scenario[bracket",
            "scenario]bracket",
            "scenario{brace",
            "scenario}brace"
        ]

        for scenario_id in invalid_ids:
            with self.assertRaises(ValueError):
                self.service._validate_scenario_id(scenario_id)

    def test_validate_id_allows_alphanumeric_dash_underscore(self):
        """Test that only alphanumeric, dash, and underscore are allowed."""
        # This should pass
        self.service._validate_scenario_id("valid-scenario_123")


class TestCacheControlHeaders(TestEmissorDataServiceEndpoints):
    """Tests for cache control headers on HTTP responses."""

    def test_cache_control_headers_on_list_scenarios(self):
        """Test that cache control headers are set on list scenarios response."""
        response = self.client.get('/scenarios')

        self.assertEqual(200, response.status_code)
        self.assertIn('Cache-Control', response.headers)
        self.assertIn('no-cache', response.headers['Cache-Control'])

    def test_cache_control_headers_on_get_scenario(self):
        """Test that cache control headers are set on get scenario response."""
        self._create_test_scenario("test_scenario")

        response = self.client.get('/scenarios/test_scenario')

        self.assertEqual(200, response.status_code)
        self.assertIn('Cache-Control', response.headers)
        self.assertIn('no-cache', response.headers['Cache-Control'])

    def test_cache_control_headers_on_download(self):
        """Test that cache control headers are set on download response."""
        self._create_test_scenario("test_scenario")

        response = self.client.get('/scenarios/test_scenario/download')

        self.assertEqual(200, response.status_code)
        self.assertIn('Cache-Control', response.headers)
        self.assertIn('no-cache', response.headers['Cache-Control'])
