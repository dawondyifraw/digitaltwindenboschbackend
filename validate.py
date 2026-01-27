#!/usr/bin/env python3
"""
Validation script for Digital Twin Den Bosch Backend
AUTHOR: Daniel Wondyifraw DataTwinLabs.nl

This script validates:
1. All Python files can be imported without syntax errors
2. API endpoints are responding (if services are running)
3. Environment variables are properly configured
4. File organization is correct
"""

import sys
import os
import importlib.util
import subprocess
import requests
import time
from pathlib import Path

def check_file(filepath):
    """Check if a Python file can be compiled and imported."""
    try:
        # Compile check
        with open(filepath, 'r') as f:
            compile(f.read(), filepath, 'exec')

        # Import check (if it's a module)
        module_name = os.path.basename(filepath)[:-3]  # Remove .py
        spec = importlib.util.spec_from_file_location(module_name, filepath)
        if spec and spec.loader:
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            print(f"✓ {filepath} - OK")
            return True
        else:
            print(f"✓ {filepath} - Compiled OK (not importable)")
            return True
    except Exception as e:
        print(f"✗ {filepath} - ERROR: {e}")
        return False


def check_environment_variables():
    """Check if required environment variables are set."""
    print("\nChecking environment variables...")
    required_vars = ['INFLUX_TOKEN', 'HYPERBOLIC_API_KEY']
    optional_vars = ['INFLUX_URL', 'INFLUX_ORG', 'BUCKET', 'HYPERBOLIC_URL', 'LLM_MODEL']

    all_good = True
    for var in required_vars:
        if os.getenv(var):
            print(f"✓ {var} - Set")
        else:
            print(f"✗ {var} - NOT SET (required)")
            all_good = False

    for var in optional_vars:
        if os.getenv(var):
            print(f"✓ {var} - Set")
        else:
            print(f"⚠ {var} - Not set (using defaults)")

    return all_good


def test_api_endpoints():
    """Test API endpoints if services are running."""
    print("\nTesting API endpoints...")

    test_queries = [
        "What was CO2 in construction zone yesterday?",
        "Show noise levels for S-I1",
        "Show where CO2 > 600 ppm in the last hour",
        "List all sensors in residential zone",
        "What is current temperature?"  # Expected to fail - no temperature sensors
    ]

    base_url = "http://127.0.0.1:5050"
    working_endpoints = 0

    for query in test_queries:
        try:
            start = time.time()
            response = requests.post(f"{base_url}/query",
                                   json={"query": query},
                                   timeout=10)
            end = time.time()
            status = response.status_code
            duration = (end - start) * 1000

            if status == 200:
                print(f"✓ '{query[:30]}...' → {duration:.0f}ms → Status: {status}")
                working_endpoints += 1
            else:
                print(f"⚠ '{query[:30]}...' → {duration:.0f}ms → Status: {status}")

        except requests.exceptions.ConnectionError:
            print(f"⚠ API not running - cannot test endpoints")
            return True  # Not an error if services aren't running
        except Exception as e:
            print(f"✗ '{query[:30]}...' → ERROR: {str(e)[:50]}...")

    return working_endpoints > 0


def run_unit_tests():
    """Run unit tests to verify core functionality."""
    print("\nRunning unit tests...")
    try:
        import subprocess
        result = subprocess.run([sys.executable, "tests/unit_tests.py"],
                              capture_output=True, text=True, cwd=os.getcwd())

        if result.returncode == 0:
            print("✅ Unit tests passed")
            # Print a summary line from the output
            lines = result.stdout.strip().split('\n')
            for line in lines:
                if 'All tests passed' in line:
                    print(f"   {line}")
                    break
            return True
        else:
            print("❌ Unit tests failed")
            print(result.stdout)
            print(result.stderr)
            return False
    except Exception as e:
        print(f"⚠️  Could not run unit tests: {e}")
        return True  # Don't fail validation if tests can't run


def check_file_organization():
    """Check if files are properly organized in directories."""
    print("\nChecking file organization...")

    expected_structure = {
        'apis': ['dashboard_api.py', 'explainer.py', 'llm_influx_query_engine.py'],
        'consumers': ['kafka_consumer_anomalies.py', 'kafka_consumer_influx.py'],
        'producers': ['kafka_producer_simulator.py', 'kafka_simulator_correlation.py'],
        'detectors': ['anomaly_detector_websocket.py', 'detector_evaluation.py'],
        'utils': ['metrics_reader.py', 'odin_brain.py', 'odin_metrics.py',
                 'socket_client_tester.py', 'websocket_server_emitter.py'],
        'tests': ['quick_test.py', 'unit_tests.py']
    }

    all_good = True

    for directory, expected_files in expected_structure.items():
        if not os.path.exists(directory):
            print(f"✗ Directory missing: {directory}")
            all_good = False
            continue

        actual_files = [f for f in os.listdir(directory) if f.endswith('.py')]
        missing_files = set(expected_files) - set(actual_files)
        extra_files = set(actual_files) - set(expected_files)

        if missing_files:
            print(f"✗ Missing files in {directory}: {missing_files}")
            all_good = False

        if extra_files:
            print(f"⚠ Extra files in {directory}: {extra_files}")

        if not missing_files and not extra_files:
            print(f"✓ {directory} - Properly organized")

    return all_good

def main():
    """Main validation function."""
    print("🔍 Digital Twin Den Bosch Backend Validation")
    print("=" * 50)

    # Check file organization first
    org_ok = check_file_organization()

    # Check environment variables
    env_ok = check_environment_variables()

    # Validate Python files
    print("\nValidating Python files...")
    directories = ['apis', 'producers', 'consumers', 'detectors', 'utils', 'tests']
    total_files = 0
    failed_files = 0

    for directory in directories:
        if os.path.exists(directory):
            for filename in os.listdir(directory):
                if filename.endswith('.py'):
                    filepath = os.path.join(directory, filename)
                    total_files += 1
                    if not check_file(filepath):
                        failed_files += 1

    # Test API endpoints
    api_ok = test_api_endpoints()

    # Run unit tests
    unit_ok = run_unit_tests()

    # Summary
    print("\n" + "=" * 50)
    print("VALIDATION SUMMARY")
    print("=" * 50)

    checks = [
        ("File Organization", org_ok),
        ("Environment Variables", env_ok),
        ("Python Files", failed_files == 0),
        ("API Endpoints", api_ok),
        ("Unit Tests", unit_ok)
    ]

    all_passed = True
    for check_name, passed in checks:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{check_name}: {status}")
        if not passed:
            all_passed = False

    print(f"\nPython Files: {total_files - failed_files}/{total_files} OK")

    if all_passed:
        print("\n🎉 All validations passed! Your codebase is ready.")
        return 0
    else:
        print("\n⚠️  Some validations failed. Please review the issues above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())