#!/usr/bin/env python3
"""
Comprehensive test runner for PDBe Investigation Facility.

This script provides a unified interface to run all types of tests:
- Unit tests
- Integration tests  
- Performance tests
- Stress tests

Usage:
    python run_tests.py [options]
    
Options:
    --unit          Run unit tests only
    --integration   Run integration tests only
    --performance   Run performance tests only
    --all           Run all tests (default)
    --config FILE   Use custom configuration file
    --output DIR    Output directory for test reports
    --verbose       Verbose output
    --parallel      Run tests in parallel (where supported)
"""

import argparse
import sys
import os
import json
import unittest
import time
import tempfile
import shutil
from pathlib import Path
from typing import Dict, List, Any
import logging

# Add the project root to the path
project_root = Path(__file__).parent.parent.parent.parent  # Go up to mmcif-gen root
sys.path.insert(0, str(project_root))

# Import test modules
try:
    from test_pdbe import *
    from test_pdbe_performance import *
    from test_pdbe_mock_data import MockFileGenerator
except ImportError as e:
    print(f"Error importing test modules: {e}")
    print("Make sure you're running from the project root directory")
    sys.exit(1)


class TestRunner:
    """Main test runner class."""
    
    def __init__(self, config_file: str = None, output_dir: str = None, verbose: bool = False):
        """Initialize the test runner."""
        self.config_file = config_file or "test_config.json"
        self.output_dir = output_dir or "pdbe_test_results"
        self.verbose = verbose
        self.config = self.load_config()
        self.setup_logging()
        self.setup_output_directory()

# Loads test settings from a JSON config file. If the file is missing or invalid, uses default settings.
    def load_config(self) -> Dict[str, Any]:
        """Load test configuration."""
        try:
            with open(self.config_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"Warning: Config file {self.config_file} not found. Using defaults.")
            return self.get_default_config()
        except json.JSONDecodeError as e:
            print(f"Error parsing config file: {e}")
            return self.get_default_config()

# Provides a fallback configuration if no config file is found.
    def get_default_config(self) -> Dict[str, Any]:
        """Get default configuration."""
        return {
            "test_configuration": {
                "unit_tests": {"enabled": True, "timeout": 30},
                "integration_tests": {"enabled": True, "timeout": 120},
                "performance_tests": {"enabled": True, "timeout": 300},
                "stress_tests": {"enabled": False, "timeout": 600}
            },
            "reporting": {
                "generate_html_report": True,
                "cleanup_after_tests": True
            },
            "environment": {
                "log_level": "INFO"
            }
        }

# Configures logging to both the console and a log file, using the level from config.
    def setup_logging(self):
        """Setup logging configuration."""
        log_level = self.config.get("environment", {}).get("log_level", "INFO")
        logging.basicConfig(
            level=getattr(logging, log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler(os.path.join(self.output_dir, "test.log"))
            ]
        )
        self.logger = logging.getLogger(__name__)

# Ensures the main output directory and subdirectories exist for storing results, reports, and logs.
    def setup_output_directory(self):
        """Setup output directory for test results."""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        
        # Create subdirectories
        for subdir in ["reports", "artifacts", "logs"]:
            subdir_path = os.path.join(self.output_dir, subdir)
            if not os.path.exists(subdir_path):
                os.makedirs(subdir_path)

# Runs all unit test classes and logs the process.
    def run_unit_tests(self) -> unittest.TestResult:
        """Run unit tests."""
        self.logger.info("Starting unit tests...")
        
        # Define unit test classes
        unit_test_classes = [
            TestPdbeConfig,
            TestPdbIdValidation,
            TestInvestigationPdbeInit,
            TestInputValidation,
            TestSafeExtractField,
            TestFileUtilities,
            TestCSVParsing
        ]
        
        return self._run_test_classes(unit_test_classes, "Unit Tests")
    
    def run_integration_tests(self) -> unittest.TestResult:
        """Run integration tests."""
        self.logger.info("Starting integration tests...")
        
        integration_test_classes = [
            TestBatchProcessing,
            TestDownloadFunctionality,
            TestIntegration
        ]
        
        return self._run_test_classes(integration_test_classes, "Integration Tests")
    
    def run_performance_tests(self) -> unittest.TestResult:
        """Run performance tests."""
        self.logger.info("Starting performance tests...")
        
        performance_test_classes = [
            TestBatchProcessingPerformance,
            TestMemoryUsagePatterns,
            TestScalabilityLimits,
            TestRealWorldScenarios
        ]
        
        return self._run_test_classes(performance_test_classes, "Performance Tests")
    
    def run_stress_tests(self) -> unittest.TestResult:
        """Run stress tests."""
        self.logger.info("Starting stress tests...")
        
        stress_test_classes = [
            TestConcurrencyAndThreadSafety,
        ]
        
        return self._run_test_classes(stress_test_classes, "Stress Tests")

#  Loads and runs all tests from the provided classes, prints and logs results, and returns the outcome.
    def _run_test_classes(self, test_classes: List, suite_name: str) -> unittest.TestResult:
        """Run a set of test classes."""
        suite = unittest.TestSuite()
        
        for test_class in test_classes:
            tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
            suite.addTests(tests)
        
        # Create a custom test runner
        runner = unittest.TextTestRunner(
            verbosity=2 if self.verbose else 1,
            stream=sys.stdout,
            buffer=True
        )
        
        print(f"\n{'='*60}")
        print(f"Running {suite_name}")
        print(f"{'='*60}")
        
        start_time = time.time()
        result = runner.run(suite)
        end_time = time.time()
        
        # Log results
        self.logger.info(f"{suite_name} completed in {end_time - start_time:.2f} seconds")
        self.logger.info(f"Tests run: {result.testsRun}, Failures: {len(result.failures)}, Errors: {len(result.errors)}")
        
        return result

# If enabled in config, generates an HTML report summarizing test results.    
    def generate_html_report(self, results: Dict[str, unittest.TestResult]):
        """Generate HTML test report."""
        if not self.config.get("reporting", {}).get("generate_html_report", False):
            return
        
        try:
            html_content = self._create_html_report(results)
            report_file = os.path.join(self.output_dir, "reports", "test_report.html")
            
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            self.logger.info(f"HTML report generated: {report_file}")
        except Exception as e:
            self.logger.error(f"Failed to generate HTML report: {e}")
            # Continue without HTML report
    
# Creates HTML content for the test report, including headers, summaries, and test results.
    def _create_html_report(self, results: Dict[str, unittest.TestResult]) -> str:
        """Create HTML report content."""
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>PDBe Test Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .header {{ background-color: #f0f0f0; padding: 20px; border-radius: 5px; }}
        .summary {{ margin: 20px 0; }}
        .test-suite {{ margin: 20px 0; border: 1px solid #ddd; border-radius: 5px; }}
        .suite-header {{ background-color: #e8e8e8; padding: 10px; font-weight: bold; }}
        .test-results {{ padding: 10px; }}
        .success {{ color: green; }}
        .failure {{ color: red; }}
        .error {{ color: orange; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>PDBe Investigation Facility - Test Report</h1>
        <p>Generated on: {timestamp}</p>
    </div>
    
    <div class="summary">
        <h2>Test Summary</h2>
        <table>
            <tr><th>Test Suite</th><th>Tests Run</th><th>Passed</th><th>Failed</th><th>Errors</th><th>Success Rate</th></tr>
"""
        
        total_tests = 0
        total_failures = 0
        total_errors = 0
        
        for suite_name, result in results.items():
            if result:
                passed = result.testsRun - len(result.failures) - len(result.errors)
                success_rate = (passed / result.testsRun * 100) if result.testsRun > 0 else 0
                
                html += f"""
            <tr>
                <td>{suite_name}</td>
                <td>{result.testsRun}</td>
                <td class="success">{passed}</td>
                <td class="failure">{len(result.failures)}</td>
                <td class="error">{len(result.errors)}</td>
                <td>{success_rate:.1f}%</td>
            </tr>
"""
                total_tests += result.testsRun
                total_failures += len(result.failures)
                total_errors += len(result.errors)
        
        total_passed = total_tests - total_failures - total_errors
        overall_success_rate = (total_passed / total_tests * 100) if total_tests > 0 else 0
        
        html += f"""
            <tr style="font-weight: bold; background-color: #f9f9f9;">
                <td>TOTAL</td>
                <td>{total_tests}</td>
                <td class="success">{total_passed}</td>
                <td class="failure">{total_failures}</td>
                <td class="error">{total_errors}</td>
                <td>{overall_success_rate:.1f}%</td>
            </tr>
        </table>
    </div>
"""
        
        # Add detailed results for each suite
        for suite_name, result in results.items():
            if result and (result.failures or result.errors):
                html += f"""
    <div class="test-suite">
        <div class="suite-header">{suite_name} - Detailed Results</div>
        <div class="test-results">
"""
                
                if result.failures:
                    html += "<h4>Failures:</h4><ul>"
                    for test, traceback in result.failures:
                        html += f"<li><strong>{test}</strong><br><pre>{traceback}</pre></li>"
                    html += "</ul>"
                
                if result.errors:
                    html += "<h4>Errors:</h4><ul>"
                    for test, traceback in result.errors:
                        html += f"<li><strong>{test}</strong><br><pre>{traceback}</pre></li>"
                    html += "</ul>"
                
                html += """
        </div>
    </div>
"""
        
        html += """
</body>
</html>
"""
        return html
    
    def cleanup(self):
        """Cleanup test artifacts if configured."""
        if self.config.get("reporting", {}).get("cleanup_after_tests", False):
            # Clean up temporary files but keep reports
            artifacts_dir = os.path.join(self.output_dir, "artifacts")
            if os.path.exists(artifacts_dir):
                try:
                    shutil.rmtree(artifacts_dir)
                    os.makedirs(artifacts_dir)
                except (PermissionError, OSError) as e:
                    self.logger.warning(f"Could not clean up artifacts directory: {e}")
                    # Continue without cleanup
    
    def run_all_tests(self) -> Dict[str, unittest.TestResult]:
        """Run all enabled test suites."""
        results = {}
        config = self.config.get("test_configuration", {})
        
        if config.get("unit_tests", {}).get("enabled", True):
            results["Unit Tests"] = self.run_unit_tests()
        
        if config.get("integration_tests", {}).get("enabled", True):
            results["Integration Tests"] = self.run_integration_tests()
        
        if config.get("performance_tests", {}).get("enabled", True):
            results["Performance Tests"] = self.run_performance_tests()
        
        if config.get("stress_tests", {}).get("enabled", False):
            results["Stress Tests"] = self.run_stress_tests()
        
        return results


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="PDBe Test Runner")
    parser.add_argument("--unit", action="store_true", help="Run unit tests only")
    parser.add_argument("--integration", action="store_true", help="Run integration tests only")
    parser.add_argument("--performance", action="store_true", help="Run performance tests only")
    parser.add_argument("--stress", action="store_true", help="Run stress tests only")
    parser.add_argument("--all", action="store_true", help="Run all tests (default)")
    parser.add_argument("--config", help="Custom configuration file")
    parser.add_argument("--output", help="Output directory for test reports")
    parser.add_argument("--verbose", action="store_true", help="Verbose output")
    parser.add_argument("--parallel", action="store_true", help="Run tests in parallel")
    
    args = parser.parse_args()
    
    # Default to running all tests if no specific test type is specified
    if not any([args.unit, args.integration, args.performance, args.stress]):
        args.all = True
    
    # Initialize test runner
    runner = TestRunner(
        config_file=args.config,
        output_dir=args.output,
        verbose=args.verbose
    )
    
    try:
        results = {}
        
        if args.all:
            results = runner.run_all_tests()
        else:
            if args.unit:
                results["Unit Tests"] = runner.run_unit_tests()
            if args.integration:
                results["Integration Tests"] = runner.run_integration_tests()
            if args.performance:
                results["Performance Tests"] = runner.run_performance_tests()
            if args.stress:
                results["Stress Tests"] = runner.run_stress_tests()
        
        # Generate reports
        runner.generate_html_report(results)
        
        # Print final summary
        print(f"\n{'='*80}")
        print("FINAL TEST SUMMARY")
        print(f"{'='*80}")
        
        total_tests = 0
        total_failures = 0
        total_errors = 0
        
        for suite_name, result in results.items():
            if result:
                passed = result.testsRun - len(result.failures) - len(result.errors)
                print(f"{suite_name:20} | Tests: {result.testsRun:3d} | Passed: {passed:3d} | Failed: {len(result.failures):3d} | Errors: {len(result.errors):3d}")
                
                total_tests += result.testsRun
                total_failures += len(result.failures)
                total_errors += len(result.errors)
        
        total_passed = total_tests - total_failures - total_errors
        success_rate = (total_passed / total_tests * 100) if total_tests > 0 else 0
        
        print(f"{'='*80}")
        print(f"{'TOTAL':20} | Tests: {total_tests:3d} | Passed: {total_passed:3d} | Failed: {total_failures:3d} | Errors: {total_errors:3d}")
        print(f"Overall Success Rate: {success_rate:.1f}%")
        print(f"{'='*80}")
        
        # Cleanup
        runner.cleanup()
        
        # Exit with appropriate code
        if total_failures > 0 or total_errors > 0:
            sys.exit(1)
        else:
            sys.exit(0)
    
    except KeyboardInterrupt:
        print("\nTest run interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"Error running tests: {str(e)}")
        import traceback
        print("Full traceback:")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
