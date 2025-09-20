"""
Performance and stress tests for PDBe Investigation Facility.

This module contains tests for:
- Memory usage under load
- Processing speed benchmarks
- Batch processing efficiency
- Large dataset handling
- Concurrent operation testing
"""

import unittest
import time
import psutil
import os
import tempfile
import shutil
from unittest.mock import Mock, patch, MagicMock
import threading
from typing import List, Dict

# Import test utilities
from test_pdbe_mock_data import MockFileGenerator, MockDatabaseGenerator, MockmmCIFGenerator

# Import the module under test
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent.parent  # Go up to mmcif-gen root
sys.path.insert(0, str(project_root))

from mmcif_gen.facilities.pdbe import InvestigationPdbe, PdbeConfig


class PerformanceTestBase(unittest.TestCase):
    """Base class for performance tests with common utilities."""
    
    def setUp(self):
        """Set up performance testing environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.mock_generator = MockFileGenerator()
        self.start_memory = self.get_memory_usage()
        self.start_time = time.time()
    
    def tearDown(self):
        """Clean up and report performance metrics."""
        end_time = time.time()
        end_memory = self.get_memory_usage()
        
        execution_time = end_time - self.start_time
        memory_delta = end_memory - self.start_memory
        
        print(f"\n--- Performance Metrics for {self._testMethodName} ---")
        print(f"Execution time: {execution_time:.3f} seconds")
        print(f"Memory delta: {memory_delta:.2f} MB")
        print(f"Peak memory: {end_memory:.2f} MB")
        
        # Cleanup
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
        self.mock_generator.cleanup()
    
    @staticmethod
    def get_memory_usage() -> float:
        """Get current memory usage in MB."""
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / 1024 / 1024
    
    def measure_execution_time(self, func, *args, **kwargs):
        """Measure execution time of a function."""
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        return result, end - start


class TestBatchProcessingPerformance(PerformanceTestBase):
    """Test batch processing performance and scalability."""
    
    @patch('mmcif_gen.facilities.pdbe.CIFReader')
    @patch('mmcif_gen.facilities.pdbe.SqliteReader')
    def test_small_batch_performance(self, mock_sqlite, mock_cif):
        """Test performance with small batches (baseline)."""
        self._test_batch_performance(mock_sqlite, mock_cif, batch_size=10, entity_count=100)
    
    @patch('mmcif_gen.facilities.pdbe.CIFReader')
    @patch('mmcif_gen.facilities.pdbe.SqliteReader')
    def test_medium_batch_performance(self, mock_sqlite, mock_cif):
        """Test performance with medium batches."""
        self._test_batch_performance(mock_sqlite, mock_cif, batch_size=100, entity_count=1000)
    
    @patch('mmcif_gen.facilities.pdbe.CIFReader')
    @patch('mmcif_gen.facilities.pdbe.SqliteReader')
    def test_large_batch_performance(self, mock_sqlite, mock_cif):
        """Test performance with large batches."""
        self._test_batch_performance(mock_sqlite, mock_cif, batch_size=1000, entity_count=10000)
    
    def _test_batch_performance(self, mock_sqlite, mock_cif, batch_size: int, entity_count: int):
        """Helper method to test batch processing performance."""
        # Mock database operations
        mock_cursor = MagicMock()
        mock_sqlite.return_value.sqlite_db_connection.return_value.__enter__.return_value = mock_cursor
        
        # Create processor with specific batch size
        config = PdbeConfig(batch_size=batch_size)
        processor = InvestigationPdbe(
            ["/dummy/file.cif"],
            "test_investigation",
            self.temp_dir,
            "/dummy/operation.json",
            config=config
        )
        
        # Generate test data
        test_data = MockDatabaseGenerator.generate_entity_records(entity_count)
        
        # Measure batch processing performance
        start_memory = self.get_memory_usage()
        result, execution_time = self.measure_execution_time(
            processor.process_entities_in_batches, test_data
        )
        end_memory = self.get_memory_usage()
        
        # Calculate expected number of batches
        expected_batches = (entity_count + batch_size - 1) // batch_size
        
        # Verify performance characteristics
        self.assertEqual(mock_cursor.executemany.call_count, expected_batches)
        
        # Performance assertions
        memory_growth = end_memory - start_memory
        entities_per_second = entity_count / execution_time if execution_time > 0 else 0
        
        print(f"Batch size: {batch_size}, Entities: {entity_count}")
        print(f"Batches created: {expected_batches}")
        print(f"Memory growth: {memory_growth:.2f} MB")
        print(f"Processing rate: {entities_per_second:.0f} entities/second")
        
        # Memory growth should be reasonable (less than 10MB for this test)
        self.assertLess(memory_growth, 10.0, "Memory growth too high")
        
        # Processing should be reasonably fast (at least 100 entities/second)
        self.assertGreater(entities_per_second, 100, "Processing too slow")


class TestMemoryUsagePatterns(PerformanceTestBase):
    """Test memory usage patterns and potential leaks."""
    
    @patch('mmcif_gen.facilities.pdbe.CIFReader')
    @patch('mmcif_gen.facilities.pdbe.SqliteReader')
    def test_memory_stability_over_time(self, mock_sqlite, mock_cif):
        """Test that memory usage remains stable over multiple operations."""
        mock_cursor = MagicMock()
        mock_sqlite.return_value.sqlite_db_connection.return_value.__enter__.return_value = mock_cursor
        
        config = PdbeConfig(batch_size=100)
        processor = InvestigationPdbe(
            ["/dummy/file.cif"],
            "test_investigation",
            self.temp_dir,
            "/dummy/operation.json",
            config=config
        )
        
        memory_measurements = []
        
        # Perform multiple batch operations
        for iteration in range(10):
            test_data = MockDatabaseGenerator.generate_entity_records(500)
            processor.process_entities_in_batches(test_data)
            
            # Measure memory after each iteration
            memory_measurements.append(self.get_memory_usage())
            
            # Small delay to allow garbage collection
            time.sleep(0.1)
        
        # Analyze memory growth pattern
        initial_memory = memory_measurements[0]
        final_memory = memory_measurements[-1]
        max_memory = max(memory_measurements)
        
        memory_growth = final_memory - initial_memory
        peak_growth = max_memory - initial_memory
        
        print(f"Initial memory: {initial_memory:.2f} MB")
        print(f"Final memory: {final_memory:.2f} MB")
        print(f"Total growth: {memory_growth:.2f} MB")
        print(f"Peak growth: {peak_growth:.2f} MB")
        
        # Memory growth should be minimal (less than 5MB)
        self.assertLess(memory_growth, 5.0, "Potential memory leak detected")
        self.assertLess(peak_growth, 10.0, "Peak memory usage too high")
    
    @patch('mmcif_gen.facilities.pdbe.CIFReader')
    @patch('mmcif_gen.facilities.pdbe.SqliteReader')
    def test_streaming_vs_bulk_memory_usage(self, mock_sqlite, mock_cif):
        """Compare memory usage between streaming and bulk processing."""
        mock_cursor = MagicMock()
        mock_sqlite.return_value.sqlite_db_connection.return_value.__enter__.return_value = mock_cursor
        
        # Test streaming approach (current implementation)
        config_streaming = PdbeConfig(batch_size=100)
        processor_streaming = InvestigationPdbe(
            ["/dummy/file.cif"],
            "test_investigation",
            self.temp_dir,
            "/dummy/operation.json",
            config=config_streaming
        )
        
        test_data = MockDatabaseGenerator.generate_entity_records(5000)
        
        # Measure streaming memory usage
        start_memory = self.get_memory_usage()
        processor_streaming.process_entities_in_batches(test_data)
        streaming_memory = self.get_memory_usage() - start_memory
        
        print(f"Streaming memory usage: {streaming_memory:.2f} MB")
        
        # Streaming should use minimal memory (less than 5MB for 5000 records)
        self.assertLess(streaming_memory, 5.0, "Streaming memory usage too high")


class TestConcurrencyAndThreadSafety(PerformanceTestBase):
    """Test concurrent operations and thread safety."""
    
    @patch('mmcif_gen.facilities.pdbe.CIFReader')
    @patch('mmcif_gen.facilities.pdbe.SqliteReader')
    def test_concurrent_batch_processing(self, mock_sqlite, mock_cif):
        """Test concurrent batch processing operations."""
        # Note: Current implementation is not thread-safe by design
        # This test documents the current behavior and can be updated
        # if thread safety is added in the future
        
        mock_cursor = MagicMock()
        mock_sqlite.return_value.sqlite_db_connection.return_value.__enter__.return_value = mock_cursor
        
        config = PdbeConfig(batch_size=50)
        
        results = []
        errors = []
        
        def process_batch(thread_id: int):
            """Process a batch in a separate thread."""
            try:
                processor = InvestigationPdbe(
                    [f"/dummy/file_{thread_id}.cif"],
                    f"test_investigation_{thread_id}",
                    self.temp_dir,
                    "/dummy/operation.json",
                    config=config
                )
                
                test_data = MockDatabaseGenerator.generate_entity_records(200)
                start_time = time.time()
                processor.process_entities_in_batches(test_data)
                end_time = time.time()
                
                results.append({
                    'thread_id': thread_id,
                    'execution_time': end_time - start_time,
                    'records_processed': len(test_data)
                })
            except Exception as e:
                errors.append({
                    'thread_id': thread_id,
                    'error': str(e)
                })
        
        # Create and start multiple threads
        threads = []
        num_threads = 3
        
        for i in range(num_threads):
            thread = threading.Thread(target=process_batch, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Analyze results
        print(f"Completed threads: {len(results)}")
        print(f"Failed threads: {len(errors)}")
        
        if results:
            avg_time = sum(r['execution_time'] for r in results) / len(results)
            total_records = sum(r['records_processed'] for r in results)
            print(f"Average execution time: {avg_time:.3f} seconds")
            print(f"Total records processed: {total_records}")
        
        if errors:
            print("Errors encountered:")
            for error in errors:
                print(f"  Thread {error['thread_id']}: {error['error']}")
        
        # At minimum, we should have some successful results
        # (This test mainly documents current behavior)
        self.assertGreaterEqual(len(results), 1, "No successful concurrent operations")


class TestScalabilityLimits(PerformanceTestBase):
    """Test scalability limits and edge cases."""
    
    @patch('mmcif_gen.facilities.pdbe.CIFReader')
    @patch('mmcif_gen.facilities.pdbe.SqliteReader')
    def test_maximum_batch_size_handling(self, mock_sqlite, mock_cif):
        """Test handling of very large batch sizes."""
        mock_cursor = MagicMock()
        mock_sqlite.return_value.sqlite_db_connection.return_value.__enter__.return_value = mock_cursor
        
        # Test with very large batch size
        config = PdbeConfig(batch_size=50000)  # Very large batch
        processor = InvestigationPdbe(
            ["/dummy/file.cif"],
            "test_investigation",
            self.temp_dir,
            "/dummy/operation.json",
            config=config
        )
        
        # Generate large dataset
        test_data = MockDatabaseGenerator.generate_entity_records(10000)
        
        start_memory = self.get_memory_usage()
        start_time = time.time()
        
        processor.process_entities_in_batches(test_data)
        
        end_time = time.time()
        end_memory = self.get_memory_usage()
        
        execution_time = end_time - start_time
        memory_usage = end_memory - start_memory
        
        print(f"Large batch processing:")
        print(f"  Records: {len(test_data)}")
        print(f"  Batch size: {config.batch_size}")
        print(f"  Execution time: {execution_time:.3f} seconds")
        print(f"  Memory usage: {memory_usage:.2f} MB")
        
        # Should complete successfully even with large batches
        self.assertEqual(mock_cursor.executemany.call_count, 1)  # Single batch
        
        # Performance should still be reasonable
        self.assertLess(execution_time, 5.0, "Large batch processing too slow")
        self.assertLess(memory_usage, 50.0, "Large batch memory usage too high")
    
    @patch('mmcif_gen.facilities.pdbe.CIFReader')
    @patch('mmcif_gen.facilities.pdbe.SqliteReader')
    def test_minimum_batch_size_efficiency(self, mock_sqlite, mock_cif):
        """Test efficiency with very small batch sizes."""
        mock_cursor = MagicMock()
        mock_sqlite.return_value.sqlite_db_connection.return_value.__enter__.return_value = mock_cursor
        
        # Test with very small batch size
        config = PdbeConfig(batch_size=1)  # Process one record at a time
        processor = InvestigationPdbe(
            ["/dummy/file.cif"],
            "test_investigation",
            self.temp_dir,
            "/dummy/operation.json",
            config=config
        )
        
        test_data = MockDatabaseGenerator.generate_entity_records(100)
        
        start_time = time.time()
        processor.process_entities_in_batches(test_data)
        end_time = time.time()
        
        execution_time = end_time - start_time
        
        print(f"Small batch processing:")
        print(f"  Records: {len(test_data)}")
        print(f"  Batch size: {config.batch_size}")
        print(f"  Execution time: {execution_time:.3f} seconds")
        print(f"  Database calls: {mock_cursor.executemany.call_count}")
        
        # Should make one database call per record
        self.assertEqual(mock_cursor.executemany.call_count, len(test_data))
        
        # Should still complete in reasonable time
        self.assertLess(execution_time, 2.0, "Small batch processing too slow")


class TestRealWorldScenarios(PerformanceTestBase):
    """Test performance with realistic data scenarios."""
    
    def test_typical_pdb_processing_simulation(self):
        """Simulate processing typical PDB files."""
        # This test simulates realistic mmCIF processing without mocking
        # to get actual performance characteristics
        
        # Create realistic test environment
        test_env = self.mock_generator.create_test_environment()
        cif_dir = os.path.join(test_env, "cif_files")
        
        # Get list of CIF files
        from mmcif_gen.facilities.pdbe import get_cif_file_paths
        
        start_time = time.time()
        start_memory = self.get_memory_usage()
        
        try:
            cif_files = get_cif_file_paths(cif_dir)
            end_time = time.time()
            end_memory = self.get_memory_usage()
            
            file_discovery_time = end_time - start_time
            memory_usage = end_memory - start_memory
            
            print(f"File discovery performance:")
            print(f"  Files found: {len(cif_files)}")
            print(f"  Discovery time: {file_discovery_time:.3f} seconds")
            print(f"  Memory usage: {memory_usage:.2f} MB")
            
            # Performance assertions for file discovery
            self.assertGreater(len(cif_files), 0, "No CIF files found")
            self.assertLess(file_discovery_time, 1.0, "File discovery too slow")
            self.assertLess(memory_usage, 5.0, "File discovery memory usage too high")
            
        except Exception as e:
            self.fail(f"File discovery failed: {e}")


if __name__ == '__main__':
    # Check if psutil is available
    try:
        import psutil
    except ImportError:
        print("Warning: psutil not available. Memory monitoring will be limited.")
        psutil = None
    
    # Create test suite focusing on performance
    test_suite = unittest.TestSuite()
    
    # Add performance test classes
    performance_test_classes = [
        TestBatchProcessingPerformance,
        TestMemoryUsagePatterns,
        TestConcurrencyAndThreadSafety,
        TestScalabilityLimits,
        TestRealWorldScenarios
    ]
    
    for test_class in performance_test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)
    
    # Run tests with detailed output
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # Print performance summary
    print(f"\n{'='*60}")
    print(f"Performance Test Summary:")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    
    if result.failures:
        print(f"\nFailures:")
        for test, traceback in result.failures:
            print(f"  {test}: {traceback.split('AssertionError:')[-1].strip()}")
    
    if result.errors:
        print(f"\nErrors:")
        for test, traceback in result.errors:
            print(f"  {test}: {traceback.split('Exception:')[-1].strip()}")
    
    print(f"{'='*60}")
