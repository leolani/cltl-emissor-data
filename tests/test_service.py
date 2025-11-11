import io
import json
import os
import unittest
import zipfile
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch, MagicMock

from flask import Flask

from cltl.emissordata.file_storage import EmissorDataFileStorage
from cltl_service.emissordata.service import EmissorDataService, MAX_SCENARIO_ID_LENGTH, MAX_ZIP_SIZE_MB
from emissor.persistence import ScenarioStorage
from emissor.representation.scenario import Scenario, TextSignal, AudioSignal, ImageSignal, Modality


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
    """Tests for GET /scenarios endpoint."""

    def test_list_scenarios_empty(self):
        """Test listing scenarios when none exist."""
        response = self.client.get('/scenarios')

        self.assertEqual(200, response.status_code)
        data = json.loads(response.data)
        self.assertIn('scenarios', data)
        self.assertEqual([], data['scenarios'])

    def test_list_scenarios_single(self):
        """Test listing scenarios with one scenario."""
        scenario = self._create_test_scenario("test_scenario_1")

        response = self.client.get('/scenarios')

        self.assertEqual(200, response.status_code)
        data = json.loads(response.data)
        self.assertIn('scenarios', data)
        self.assertEqual(1, len(data['scenarios']))

        scenario_data = data['scenarios'][0]
        self.assertEqual("test_scenario_1", scenario_data['id'])
        self.assertEqual(0, scenario_data['start'])
        self.assertEqual(1000, scenario_data['end'])
        self.assertIn('context', scenario_data)

    def test_list_scenarios_multiple(self):
        """Test listing multiple scenarios."""
        self._create_test_scenario("scenario_1")
        self._create_test_scenario("scenario_2")
        self._create_test_scenario("scenario_3")

        response = self.client.get('/scenarios')

        self.assertEqual(200, response.status_code)
        data = json.loads(response.data)
        self.assertEqual(3, len(data['scenarios']))

        scenario_ids = {s['id'] for s in data['scenarios']}
        self.assertEqual({"scenario_1", "scenario_2", "scenario_3"}, scenario_ids)

    def test_list_scenarios_with_corrupted_scenario(self):
        """Test that corrupted scenarios are skipped gracefully."""
        # Create valid scenario
        self._create_test_scenario("valid_scenario")

        # Create corrupted scenario by creating directory but invalid data
        corrupted_path = os.path.join(self.storage_path, "corrupted_scenario")
        os.makedirs(corrupted_path, exist_ok=True)

        # Write invalid JSON to scenario file
        scenario_file = os.path.join(corrupted_path, "scenario.json")
        with open(scenario_file, 'w') as f:
            f.write("invalid json content")

        response = self.client.get('/scenarios')

        # Should succeed but only return valid scenario
        self.assertEqual(200, response.status_code)
        data = json.loads(response.data)
        self.assertEqual(1, len(data['scenarios']))
        self.assertEqual("valid_scenario", data['scenarios'][0]['id'])

    def test_list_scenarios_internal_error_handling(self):
        """Test that internal errors return 500."""
        # Mock the storage to raise an exception
        with patch.object(self.service, '_list_scenarios', side_effect=RuntimeError("Test error")):
            response = self.client.get('/scenarios')

            self.assertEqual(500, response.status_code)
            data = json.loads(response.data)
            self.assertIn('error', data)
            self.assertEqual("Internal server error", data['error'])


class TestGetScenarioEndpoint(TestEmissorDataServiceEndpoints):
    """Tests for GET /scenarios/<scenario_id> endpoint."""

    def test_get_scenario_success(self):
        """Test getting a scenario with all signals."""
        self._create_test_scenario("test_scenario")

        response = self.client.get('/scenarios/test_scenario')

        self.assertEqual(200, response.status_code)
        data = json.loads(response.data)

        # Verify scenario data
        self.assertIn('scenario', data)
        self.assertIn('signals', data)

        scenario = data['scenario']
        self.assertEqual("test_scenario", scenario['id'])
        self.assertEqual(0, scenario['ruler']['start'])
        self.assertEqual(1000, scenario['ruler']['end'])

        # Verify signals
        signals = data['signals']
        self.assertIn('text', signals)
        self.assertIn('audio', signals)
        self.assertIn('image', signals)

        self.assertEqual(1, len(signals['text']))
        self.assertEqual(1, len(signals['audio']))
        self.assertEqual(1, len(signals['image']))

    def test_get_scenario_not_found(self):
        """Test getting non-existent scenario returns 404."""
        response = self.client.get('/scenarios/nonexistent_scenario')

        self.assertEqual(404, response.status_code)
        data = json.loads(response.data)
        self.assertIn('error', data)
        self.assertEqual("Scenario not found", data['error'])

    def test_get_scenario_no_signals(self):
        """Test getting scenario with no signals."""
        self._create_test_scenario("empty_scenario", include_signals=False)

        response = self.client.get('/scenarios/empty_scenario')

        self.assertEqual(200, response.status_code)
        data = json.loads(response.data)

        self.assertIn('scenario', data)
        self.assertIn('signals', data)
        self.assertEqual({'audio': [], 'image': [], 'text': []}, data['signals'])

    def test_get_scenario_partial_modalities(self):
        """Test scenario with only some modalities."""
        scenario_id = "partial_scenario"
        scenario = Scenario.new_instance(scenario_id, 0, 1000, "test", {"text": "./text"})
        self.emissor_storage.start_scenario(scenario)

        # Only add text signal
        text_signal = TextSignal.for_scenario(scenario_id, 0, 100, "Only text")
        self.emissor_storage.add_signal(text_signal)

        self.emissor_storage.stop_scenario(scenario)

        response = self.client.get(f'/scenarios/{scenario_id}')

        self.assertEqual(200, response.status_code)
        data = json.loads(response.data)

        signals = data['signals']
        self.assertIn('text', signals)
        self.assertEqual(1, len(signals['text']))
        # Audio and image should be present with empty arrays if no signals exist
        self.assertIn('audio', signals)
        self.assertIn('image', signals)
        self.assertEqual(0, len(signals['audio']))
        self.assertEqual(0, len(signals['image']))

    def test_get_scenario_invalid_id_format(self):
        """Test that invalid scenario_id format is rejected."""
        invalid_ids = [
            '../etc/passwd',
            'scenario/../../../etc/passwd',
            'scenario@invalid',
            'scenario!test',
            'scenario with spaces',
            'scenario/slash',
            'scenario\\backslash',
        ]

        for invalid_id in invalid_ids:
            response = self.client.get(f'/scenarios/{invalid_id}')

            self.assertEqual(404, response.status_code,
                           f"Failed to reject invalid scenario_id: {invalid_id}")
            data = json.loads(response.data)
            self.assertIn('error', data)
            self.assertEqual("Scenario not found", data['error'])

    def test_get_scenario_id_too_long(self):
        """Test that scenario_id exceeding max length is rejected."""
        long_id = 'a' * (MAX_SCENARIO_ID_LENGTH + 1)

        response = self.client.get(f'/scenarios/{long_id}')

        self.assertEqual(404, response.status_code)
        data = json.loads(response.data)
        self.assertIn('error', data)

    def test_get_scenario_internal_error(self):
        """Test that internal errors return 500."""
        self._create_test_scenario("test_scenario")

        # Mock to raise exception
        with patch.object(self.service, '_get_scenario', side_effect=RuntimeError("Test error")):
            response = self.client.get('/scenarios/test_scenario')

            self.assertEqual(500, response.status_code)
            data = json.loads(response.data)
            self.assertIn('error', data)
            self.assertEqual("Internal server error", data['error'])

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
        response = self.client.get(f'/scenarios/{scenario_id}')

        self.assertEqual(200, response.status_code)
        data = json.loads(response.data)
        self.assertIn('scenario', data)


class TestDownloadScenarioEndpoint(TestEmissorDataServiceEndpoints):
    """Tests for GET /scenarios/<scenario_id>/download endpoint."""

    def test_download_scenario_success(self):
        """Test successful scenario download as zip file."""
        self._create_test_scenario("download_test")

        response = self.client.get('/scenarios/download_test/download')

        self.assertEqual(200, response.status_code)
        self.assertEqual('application/zip', response.content_type)

        # Verify it's a valid zip file
        zip_buffer = io.BytesIO(response.data)
        with zipfile.ZipFile(zip_buffer, 'r') as zip_file:
            # Check zip is valid
            self.assertIsNone(zip_file.testzip())

            # Check it contains scenario files
            file_list = zip_file.namelist()
            self.assertTrue(any('download_test' in f for f in file_list))
            self.assertTrue(any('scenario.json' in f for f in file_list))

    def test_download_scenario_not_found(self):
        """Test downloading non-existent scenario returns 404."""
        response = self.client.get('/scenarios/nonexistent/download')

        self.assertEqual(404, response.status_code)
        data = json.loads(response.data)
        self.assertIn('error', data)
        self.assertEqual("Scenario not found", data['error'])

    def test_download_scenario_path_traversal_blocked(self):
        """Test that path traversal attempts are blocked."""
        malicious_ids = [
            '../../../etc/passwd',
            '..%2F..%2F..%2Fetc%2Fpasswd',
            'scenario/../../../etc',
            '....//....//etc/passwd',
        ]

        for malicious_id in malicious_ids:
            response = self.client.get(f'/scenarios/{malicious_id}/download')

            # Should reject with 404 (validation failure)
            self.assertEqual(404, response.status_code,
                           f"Failed to block path traversal: {malicious_id}")
            data = json.loads(response.data)
            self.assertIn('error', data)

    def test_download_scenario_invalid_id_format(self):
        """Test that invalid scenario_id formats are rejected."""
        invalid_ids = [
            'scenario@test',
            'scenario!invalid',
            'scenario with spaces',
            'scenario/slash',
            'scenario\\backslash',
            'scenario;command',
            'scenario|pipe',
        ]

        for invalid_id in invalid_ids:
            response = self.client.get(f'/scenarios/{invalid_id}/download')

            self.assertEqual(404, response.status_code,
                           f"Failed to reject invalid ID: {invalid_id}")

    def test_download_scenario_too_large(self):
        """Test that scenarios exceeding size limit are rejected."""
        scenario_id = "large_scenario"
        self._create_test_scenario(scenario_id, include_signals=False)

        # Create a large dummy file in scenario directory
        scenario_path = os.path.join(self.storage_path, scenario_id)
        large_file = os.path.join(scenario_path, "large_file.dat")

        # Create file larger than MAX_ZIP_SIZE_MB
        large_size = (MAX_ZIP_SIZE_MB + 1) * 1024 * 1024
        with open(large_file, 'wb') as f:
            # Write in chunks to avoid memory issues
            chunk_size = 1024 * 1024
            for _ in range(MAX_ZIP_SIZE_MB + 1):
                f.write(b'0' * chunk_size)

        response = self.client.get(f'/scenarios/{scenario_id}/download')

        self.assertEqual(404, response.status_code)
        data = json.loads(response.data)
        self.assertIn('error', data)

    def test_download_scenario_internal_error(self):
        """Test that internal errors return 500."""
        self._create_test_scenario("test_scenario")

        with patch.object(self.service, '_create_scenario_zip', side_effect=RuntimeError("Test error")):
            response = self.client.get('/scenarios/test_scenario/download')

            self.assertEqual(500, response.status_code)
            data = json.loads(response.data)
            self.assertIn('error', data)
            self.assertEqual("Internal server error", data['error'])

    def test_download_scenario_empty(self):
        """Test downloading scenario with minimal content."""
        scenario_id = "minimal_scenario"
        self._create_test_scenario(scenario_id, include_signals=False)

        response = self.client.get(f'/scenarios/{scenario_id}/download')

        self.assertEqual(200, response.status_code)
        self.assertEqual('application/zip', response.content_type)

        # Should still be a valid zip
        zip_buffer = io.BytesIO(response.data)
        with zipfile.ZipFile(zip_buffer, 'r') as zip_file:
            self.assertIsNone(zip_file.testzip())


class TestValidateScenarioId(TestEmissorDataServiceEndpoints):
    """Tests for _validate_scenario_id method."""

    def test_validate_valid_ids(self):
        """Test that valid scenario IDs pass validation."""
        valid_ids = [
            'scenario1',
            'scenario_1',
            'scenario-1',
            'SCENARIO_123',
            'test-scenario_2024',
            'a' * MAX_SCENARIO_ID_LENGTH,  # Max length
        ]

        for valid_id in valid_ids:
            try:
                self.service._validate_scenario_id(valid_id)
            except ValueError:
                self.fail(f"Valid ID rejected: {valid_id}")

    def test_validate_empty_id(self):
        """Test that empty scenario_id is rejected."""
        with self.assertRaises(ValueError) as context:
            self.service._validate_scenario_id('')

        self.assertIn('Invalid scenario_id length', str(context.exception))

    def test_validate_none_id(self):
        """Test that None scenario_id is rejected."""
        with self.assertRaises((ValueError, AttributeError)):
            self.service._validate_scenario_id(None)

    def test_validate_id_too_long(self):
        """Test that scenario_id exceeding max length is rejected."""
        long_id = 'a' * (MAX_SCENARIO_ID_LENGTH + 1)

        with self.assertRaises(ValueError) as context:
            self.service._validate_scenario_id(long_id)

        self.assertIn('Invalid scenario_id length', str(context.exception))

    def test_validate_id_with_special_characters(self):
        """Test that scenario_ids with special characters are rejected."""
        invalid_ids = [
            'scenario@test',
            'scenario!invalid',
            'scenario with spaces',
            'scenario/slash',
            'scenario\\backslash',
            'scenario.dot',
            'scenario:colon',
            'scenario;semicolon',
            'scenario|pipe',
            'scenario&ampersand',
            'scenario$dollar',
            '../../../etc/passwd',
            'test\x00null',
            'test\nnewline',
        ]

        for invalid_id in invalid_ids:
            with self.assertRaises(ValueError) as context:
                self.service._validate_scenario_id(invalid_id)

            self.assertIn('Invalid scenario_id format', str(context.exception),
                         f"Failed to reject: {invalid_id}")

    def test_validate_id_allows_alphanumeric_dash_underscore(self):
        """Test that only alphanumeric, dash, and underscore are allowed."""
        # Valid characters test
        valid_id = 'Test_Scenario-123'
        try:
            self.service._validate_scenario_id(valid_id)
        except ValueError:
            self.fail(f"Valid ID rejected: {valid_id}")


class TestScenarioPathTraversalSecurity(TestEmissorDataServiceEndpoints):
    """Security tests focused on path traversal prevention."""

    def test_create_scenario_zip_path_traversal_blocked(self):
        """Test that _create_scenario_zip blocks path traversal."""
        # These should all raise ValueError due to path traversal detection
        malicious_ids = [
            '../sensitive_data',
            '../../etc',
            'valid/../../../etc',
        ]

        for malicious_id in malicious_ids:
            # First create the directory structure that would be exploited
            try:
                os.makedirs(os.path.join(self.storage_path, malicious_id), exist_ok=True)
            except:
                pass  # Some paths may fail to create, that's fine

            # Attempt to create zip should fail at validation or path check
            try:
                self.service._validate_scenario_id(malicious_id)
                # If validation passes (shouldn't), zip creation should fail
                with self.assertRaises(ValueError):
                    self.service._create_scenario_zip(malicious_id)
            except ValueError:
                pass  # Expected - validation should catch it

    def test_normalized_paths_prevent_traversal(self):
        """Test that path normalization prevents directory traversal."""
        scenario_id = "test_scenario"
        self._create_test_scenario(scenario_id)

        # Manually test the path logic from _create_scenario_zip
        storage = self.emissor_storage._storage
        base_path_normalized = os.path.normpath(storage.base_path)

        # Try to construct malicious path
        malicious_input = "../../../etc/passwd"
        scenario_path = os.path.normpath(os.path.join(base_path_normalized, malicious_input))

        # Verify it doesn't escape base path
        self.assertFalse(scenario_path.startswith(base_path_normalized + os.sep),
                        "Path traversal protection failed")

    def test_scenario_zip_validates_directory_exists(self):
        """Test that zip creation validates directory exists and is a directory."""
        # Test non-existent scenario
        with self.assertRaises(ValueError) as context:
            self.service._create_scenario_zip("nonexistent_scenario_xyz")

        self.assertIn('not found', str(context.exception))

        # Test scenario that's a file instead of directory
        file_path = os.path.join(self.storage_path, "scenario_file")
        with open(file_path, 'w') as f:
            f.write("test")

        with self.assertRaises(ValueError) as context:
            self.service._create_scenario_zip("scenario_file")

        self.assertIn('not a directory', str(context.exception))


class TestCacheControlHeaders(TestEmissorDataServiceEndpoints):
    """Tests for cache control headers on responses."""

    def test_cache_control_headers_on_list_scenarios(self):
        """Test that cache control headers are set on list scenarios response."""
        response = self.client.get('/scenarios')

        self.assertEqual('no-cache, no-store, must-revalidate', response.headers.get('Cache-Control'))
        self.assertEqual('no-cache', response.headers.get('Pragma'))
        self.assertEqual('0', response.headers.get('Expires'))

    def test_cache_control_headers_on_get_scenario(self):
        """Test that cache control headers are set on get scenario response."""
        self._create_test_scenario("test")
        response = self.client.get('/scenarios/test')

        self.assertEqual('no-cache, no-store, must-revalidate', response.headers.get('Cache-Control'))
        self.assertEqual('no-cache', response.headers.get('Pragma'))
        self.assertEqual('0', response.headers.get('Expires'))

    def test_cache_control_headers_on_download(self):
        """Test that cache control headers are set on download response."""
        self._create_test_scenario("test")
        response = self.client.get('/scenarios/test/download')

        self.assertEqual('no-cache, no-store, must-revalidate', response.headers.get('Cache-Control'))
        self.assertEqual('no-cache', response.headers.get('Pragma'))
        self.assertEqual('0', response.headers.get('Expires'))


if __name__ == '__main__':
    unittest.main()
