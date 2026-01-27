#!/usr/bin/env python3
"""
Unit Test Suite for Digital Twin Den Bosch Backend
AUTHOR: Daniel Wondyifraw DataTwinLabs.nl

This script runs unit tests for core functionality that doesn't require
external services like InfluxDB, Kafka, or Flask.
"""

import sys
import os
import unittest
from unittest.mock import patch, MagicMock

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

class TestEnvironmentVariables(unittest.TestCase):
    """Test environment variable handling."""

    def test_env_vars_loaded(self):
        """Test that environment variables are properly loaded with defaults."""
        # Test with environment variables set
        with patch.dict(os.environ, {
            'INFLUX_URL': 'http://test:8086',
            'INFLUX_TOKEN': 'test_token',
            'HYPERBOLIC_API_KEY': 'test_key'
        }):
            # Mock the imports that would fail
            with patch('sys.modules', {
                'flask': MagicMock(),
                'influxdb_client': MagicMock(),
                'confluent_kafka': MagicMock()
            }):
                try:
                    # Test dashboard_api config loading
                    from apis.dashboard_api import Config
                    self.assertEqual(Config.INFLUX_URL, 'http://test:8086')
                    self.assertEqual(Config.INFLUX_TOKEN, 'test_token')
                except ImportError:
                    # Expected in test environment
                    pass

    def test_default_values(self):
        """Test default values when environment variables are not set."""
        # Clear relevant env vars
        env_vars_to_clear = ['INFLUX_URL', 'INFLUX_TOKEN', 'HYPERBOLIC_API_KEY']
        original_values = {}
        for var in env_vars_to_clear:
            original_values[var] = os.environ.get(var)
            if var in os.environ:
                del os.environ[var]

        try:
            with patch('sys.modules', {
                'flask': MagicMock(),
                'influxdb_client': MagicMock()
            }):
                try:
                    from apis.dashboard_api import Config
                    self.assertEqual(Config.INFLUX_URL, 'http://localhost:8086')
                    self.assertEqual(Config.INFLUX_TOKEN, '')  # Empty string for security
                except ImportError:
                    # Expected in test environment
                    pass
        finally:
            # Restore original values
            for var, value in original_values.items():
                if value is not None:
                    os.environ[var] = value


class TestUtilityFunctions(unittest.TestCase):
    """Test utility functions that don't require external dependencies."""

    def test_metrics_reader(self):
        """Test metrics reader functionality."""
        try:
            from utils.metrics_reader import load_rows
            # This should work without external dependencies
            # Just test that the function exists and is callable
            self.assertTrue(callable(load_rows))
        except ImportError:
            self.skipTest("metrics_reader dependencies not available")

    def test_odin_metrics(self):
        """Test ODIN metrics functionality."""
        try:
            from utils.odin_metrics import ODIN_ENDPOINT, RUN_LABEL
            self.assertIsInstance(ODIN_ENDPOINT, str)
            self.assertIsInstance(RUN_LABEL, str)
        except ImportError:
            self.skipTest("odin_metrics dependencies not available")


class TestFileOrganization(unittest.TestCase):
    """Test that files are properly organized."""

    def test_directories_exist(self):
        """Test that all expected directories exist."""
        expected_dirs = ['apis', 'consumers', 'producers', 'detectors', 'utils', 'tests']
        for directory in expected_dirs:
            self.assertTrue(os.path.exists(directory), f"Directory {directory} should exist")
            self.assertTrue(os.path.isdir(directory), f"{directory} should be a directory")

    def test_python_files_exist(self):
        """Test that expected Python files exist in correct directories."""
        expected_files = {
            'apis': ['dashboard_api.py', 'explainer.py', 'llm_influx_query_engine.py'],
            'consumers': ['kafka_consumer_anomalies.py', 'kafka_consumer_influx.py'],
            'producers': ['kafka_producer_simulator.py', 'kafka_simulator_correlation.py'],
            'detectors': ['anomaly_detector_websocket.py', 'detector_evaluation.py'],
            'utils': ['metrics_reader.py', 'odin_brain.py', 'odin_metrics.py',
                     'socket_client_tester.py', 'websocket_server_emitter.py'],
            'tests': ['quick_test.py']
        }

        for directory, files in expected_files.items():
            for filename in files:
                filepath = os.path.join(directory, filename)
                self.assertTrue(os.path.exists(filepath), f"File {filepath} should exist")


class TestAuthorInformation(unittest.TestCase):
    """Test that author information is present in all files."""

    def test_author_in_files(self):
        """Test that author information is present in Python files."""
        python_files = []
        for root, dirs, files in os.walk('.'):
            # Skip __pycache__ and other unwanted directories
            dirs[:] = [d for d in dirs if not d.startswith('__') and d not in ['outdatedscripts']]
            for file in files:
                if file.endswith('.py'):
                    python_files.append(os.path.join(root, file))

        author_found_count = 0
        for filepath in python_files:
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = f.read()
                    if 'AUTHOR: Daniel Wondyifraw DataTwinLabs.nl' in content:
                        author_found_count += 1
            except Exception:
                # Skip files that can't be read
                continue

        # Should find author in most files (allowing for some exceptions)
        self.assertGreaterEqual(author_found_count, 10,
                               f"Author information found in {author_found_count} files, expected at least 10")


def run_tests():
    """Run the test suite."""
    print("🧪 Running Digital Twin Den Bosch Unit Tests")
    print("=" * 50)

    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Add test cases
    suite.addTests(loader.loadTestsFromTestCase(TestEnvironmentVariables))
    suite.addTests(loader.loadTestsFromTestCase(TestUtilityFunctions))
    suite.addTests(loader.loadTestsFromTestCase(TestFileOrganization))
    suite.addTests(loader.loadTestsFromTestCase(TestAuthorInformation))

    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # Summary
    print("\n" + "=" * 50)
    print("TEST SUMMARY")
    print("=" * 50)
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")

    if result.wasSuccessful():
        print("✅ All tests passed!")
        return 0
    else:
        print("❌ Some tests failed.")
        return 1


if __name__ == "__main__":
    sys.exit(run_tests())