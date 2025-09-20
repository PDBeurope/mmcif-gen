"""
Comprehensive test suite for PDBe Investigation Facility (pdbe.py)

This test suite covers:
- Unit tests for individual functions and methods
- Integration tests for complete workflows
- Performance tests for batch processing
- Error handling and edge cases
- Mock data generation for testing
"""

import unittest
from unittest.mock import Mock, patch, MagicMock, mock_open
import tempfile
import os
import shutil
import sqlite3
import json
from typing import List, Dict
import requests

# Import the module under test
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent.parent  # Go up to mmcif-gen root
sys.path.insert(0, str(project_root))

from mmcif_gen.facilities.pdbe import (
    InvestigationPdbe,
    PdbeConfig,
    validate_pdb_id,
    validate_pdb_ids,
    download_and_run_pdbe_investigation,
    cleanup_temp_files,
    get_cif_file_paths,
    parse_csv,
    run,
    run_investigation_pdbe
)


class TestPdbeConfig(unittest.TestCase):
    """Test cases for PdbeConfig class."""
    
    def test_default_config(self):
        """Test default configuration values."""
        config = PdbeConfig()
        
        self.assertEqual(config.batch_size, 1000)
        self.assertEqual(config.download_timeout, 30)
        self.assertEqual(config.max_retries, 3)
        self.assertTrue(config.validate_mmcif_format)
        self.assertTrue(config.cleanup_temp_files)
        self.assertEqual(config.log_level, "INFO")
    
    def test_custom_config(self):
        """Test custom configuration values."""
        config = PdbeConfig(
            batch_size=500,
            download_timeout=60,
            max_retries=5,
            validate_mmcif_format=False,
            cleanup_temp_files=False,
            log_level="DEBUG"
        )
        
        self.assertEqual(config.batch_size, 500)
        self.assertEqual(config.download_timeout, 60)
        self.assertEqual(config.max_retries, 5)
        self.assertFalse(config.validate_mmcif_format)
        self.assertFalse(config.cleanup_temp_files)
        self.assertEqual(config.log_level, "DEBUG")
    
    @patch('builtins.open', new_callable=mock_open, read_data='{"batch_size": 2000, "log_level": "ERROR"}')
    @patch('os.path.exists', return_value=True)
    def test_config_from_file(self, mock_exists, mock_file):
        """Test loading configuration from JSON file."""
        config = PdbeConfig.from_file("config.json")
        
        self.assertEqual(config.batch_size, 2000)
        self.assertEqual(config.log_level, "ERROR")
        # Other values should remain default
        self.assertEqual(config.download_timeout, 30)
        self.assertEqual(config.max_retries, 3)
    
    @patch('os.path.exists', return_value=False)
    def test_config_from_nonexistent_file(self, mock_exists):
        """Test loading configuration from non-existent file returns defaults."""
        config = PdbeConfig.from_file("nonexistent.json")
        
        # Should return default configuration
        self.assertEqual(config.batch_size, 1000)
        self.assertEqual(config.log_level, "INFO")


class TestPdbIdValidation(unittest.TestCase):
    """Test cases for PDB ID validation functions."""
    
    def test_valid_pdb_ids(self):
        """Test validation of valid PDB IDs."""
        valid_ids = ["1ABC", "2def", "3GHI", "4j5k", "5L6M", "7890"]
        
        for pdb_id in valid_ids:
            with self.subTest(pdb_id=pdb_id):
                self.assertTrue(validate_pdb_id(pdb_id))
    
    def test_invalid_pdb_ids(self):
        """Test validation of invalid PDB IDs."""
        invalid_ids = [
            "",           # Empty string
            "ABC",        # Too short
            "ABCDE",      # Too long
            "ABCD",       # No leading digit
            "1AB",        # Too short
            "1AB@",       # Invalid character
            None,         # None value
            "1 BC",       # Space character
        ]
        
        for pdb_id in invalid_ids:
            with self.subTest(pdb_id=pdb_id):
                self.assertFalse(validate_pdb_id(pdb_id))
    
    def test_validate_pdb_ids_list(self):
        """Test validation of PDB ID lists."""
        mixed_ids = ["1ABC", "invalid", "2DEF", "", "3GHI"]
        
        valid_ids = validate_pdb_ids(mixed_ids)
        
        self.assertEqual(len(valid_ids), 3)
        self.assertIn("1ABC", valid_ids)
        self.assertIn("2DEF", valid_ids)
        self.assertIn("3GHI", valid_ids)
    
    def test_validate_empty_pdb_ids_list(self):
        """Test validation of empty or all-invalid PDB ID list."""
        with self.assertRaises(ValueError):
            validate_pdb_ids([])
        
        with self.assertRaises(ValueError):
            validate_pdb_ids(["invalid", "also_invalid", ""])


class TestInvestigationPdbeInit(unittest.TestCase):
    """Test cases for InvestigationPdbe initialization."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.test_files = [
            os.path.join(self.temp_dir, "test1.cif"),
            os.path.join(self.temp_dir, "test2.cif")
        ]
        
        # Create test CIF files
        for file_path in self.test_files:
            with open(file_path, 'w') as f:
                f.write("data_test\n_entry.id test\n")
        
        # Create test operation file
        self.operation_file = os.path.join(self.temp_dir, "operation.json")
        with open(self.operation_file, 'w') as f:
            json.dump({"test": "data"}, f)
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir)
    
    @patch('mmcif_gen.facilities.pdbe.CIFReader')
    @patch('mmcif_gen.facilities.pdbe.SqliteReader')
    def test_init_default_config(self, mock_sqlite, mock_cif):
        """Test initialization with default configuration."""
        processor = InvestigationPdbe(
            self.test_files,
            "test_investigation",
            self.temp_dir,
            self.operation_file
        )
        
        self.assertEqual(processor.model_file_path, self.test_files)
        self.assertEqual(processor.operation_file_json, self.operation_file)
        self.assertIsInstance(processor.config, PdbeConfig)
        self.assertEqual(processor.config.batch_size, 1000)
    
    @patch('mmcif_gen.facilities.pdbe.CIFReader')
    @patch('mmcif_gen.facilities.pdbe.SqliteReader')
    def test_init_custom_config(self, mock_sqlite, mock_cif):
        """Test initialization with custom configuration."""
        custom_config = PdbeConfig(batch_size=500, log_level="DEBUG")
        
        processor = InvestigationPdbe(
            self.test_files,
            "test_investigation",
            self.temp_dir,
            self.operation_file,
            config=custom_config
        )
        
        self.assertEqual(processor.config.batch_size, 500)
        self.assertEqual(processor.config.log_level, "DEBUG")


class TestInputValidation(unittest.TestCase):
    """Test cases for input validation methods."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.valid_cif_file = os.path.join(self.temp_dir, "valid.cif")
        self.invalid_file = os.path.join(self.temp_dir, "invalid.txt")
        self.operation_file = os.path.join(self.temp_dir, "operation.json")
        
        # Create valid CIF file
        with open(self.valid_cif_file, 'w') as f:
            f.write("data_test\n")
        
        # Create invalid file
        with open(self.invalid_file, 'w') as f:
            f.write("not a cif file")
        
        # Create operation file
        with open(self.operation_file, 'w') as f:
            json.dump({"test": "data"}, f)
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir)
    
    @patch('mmcif_gen.facilities.pdbe.CIFReader')
    @patch('mmcif_gen.facilities.pdbe.SqliteReader')
    def test_validate_inputs_success(self, mock_sqlite, mock_cif):
        """Test successful input validation."""
        processor = InvestigationPdbe(
            [self.valid_cif_file],
            "test_investigation",
            self.temp_dir,
            self.operation_file
        )
        
        self.assertTrue(processor.validate_inputs())
    
    @patch('mmcif_gen.facilities.pdbe.CIFReader')
    @patch('mmcif_gen.facilities.pdbe.SqliteReader')
    def test_validate_inputs_no_files(self, mock_sqlite, mock_cif):
        """Test validation with no model files."""
        processor = InvestigationPdbe(
            [],
            "test_investigation",
            self.temp_dir,
            self.operation_file
        )
        
        self.assertFalse(processor.validate_inputs())
    
    @patch('mmcif_gen.facilities.pdbe.CIFReader')
    @patch('mmcif_gen.facilities.pdbe.SqliteReader')
    def test_validate_inputs_missing_file(self, mock_sqlite, mock_cif):
        """Test validation with missing model file."""
        processor = InvestigationPdbe(
            ["/nonexistent/file.cif"],
            "test_investigation",
            self.temp_dir,
            self.operation_file
        )
        
        self.assertFalse(processor.validate_inputs())
    
    @patch('mmcif_gen.facilities.pdbe.CIFReader')
    @patch('mmcif_gen.facilities.pdbe.SqliteReader')
    def test_validate_inputs_missing_operation_file(self, mock_sqlite, mock_cif):
        """Test validation with missing operation file."""
        processor = InvestigationPdbe(
            [self.valid_cif_file],
            "test_investigation",
            self.temp_dir,
            "/nonexistent/operation.json"
        )
        
        self.assertFalse(processor.validate_inputs())


class TestSafeExtractField(unittest.TestCase):
    """Test cases for safe field extraction method."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.operation_file = os.path.join(self.temp_dir, "operation.json")
        
        with open(self.operation_file, 'w') as f:
            json.dump({"test": "data"}, f)
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir)
    
    @patch('mmcif_gen.facilities.pdbe.CIFReader')
    @patch('mmcif_gen.facilities.pdbe.SqliteReader')
    def test_safe_extract_field_success(self, mock_sqlite, mock_cif):
        """Test successful field extraction."""
        processor = InvestigationPdbe(
            ["/dummy/file.cif"],
            "test_investigation",
            self.temp_dir,
            self.operation_file
        )
        
        row = ["value1", "value2", "value3"]
        columns = {"field1": 0, "field2": 1, "field3": 2}
        
        result = processor.safe_extract_field(row, columns, "field2")
        self.assertEqual(result, "value2")
    
    @patch('mmcif_gen.facilities.pdbe.CIFReader')
    @patch('mmcif_gen.facilities.pdbe.SqliteReader')
    def test_safe_extract_field_missing_field(self, mock_sqlite, mock_cif):
        """Test field extraction with missing field."""
        processor = InvestigationPdbe(
            ["/dummy/file.cif"],
            "test_investigation",
            self.temp_dir,
            self.operation_file
        )
        
        row = ["value1", "value2"]
        columns = {"field1": 0, "field2": 1}
        
        result = processor.safe_extract_field(row, columns, "missing_field", "default")
        self.assertEqual(result, "default")
    
    @patch('mmcif_gen.facilities.pdbe.CIFReader')
    @patch('mmcif_gen.facilities.pdbe.SqliteReader')
    def test_safe_extract_field_index_error(self, mock_sqlite, mock_cif):
        """Test field extraction with index error."""
        processor = InvestigationPdbe(
            ["/dummy/file.cif"],
            "test_investigation",
            self.temp_dir,
            self.operation_file
        )
        
        row = ["value1"]  # Short row
        columns = {"field1": 0, "field2": 5}  # Index 5 doesn't exist
        
        result = processor.safe_extract_field(row, columns, "field2", "default")
        self.assertEqual(result, "default")


class TestBatchProcessing(unittest.TestCase):
    """Test cases for batch processing functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.operation_file = os.path.join(self.temp_dir, "operation.json")
        
        with open(self.operation_file, 'w') as f:
            json.dump({"test": "data"}, f)
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir)
    
    @patch('mmcif_gen.facilities.pdbe.CIFReader')
    @patch('mmcif_gen.facilities.pdbe.SqliteReader')
    def test_insert_batch_success(self, mock_sqlite, mock_cif):
        """Test successful batch insertion."""
        # Mock the database cursor
        mock_cursor = MagicMock()
        mock_sqlite.return_value.sqlite_db_connection.return_value.__enter__.return_value = mock_cursor
        
        processor = InvestigationPdbe(
            ["/dummy/file.cif"],
            "test_investigation",
            self.temp_dir,
            self.operation_file
        )
        
        batch_data = [
            {
                "ordinal": 1,
                "pdb_id": "1ABC",
                "file_name": "test.cif",
                "model_file_no": "",
                "entity_id": "1",
                "type": "polymer",
                "seq_one_letter_code": "ACGT",
                "seq_one_letter_code_can": "ACGT",
                "chem_comp_id": "",
                "src_method": "nat",
                "description": "Test protein",
                "poly_type": "protein",
                "organism_scientific": "Homo sapiens",
                "ncbi_taxonomy_id": "9606"
            }
        ]
        
        # Should not raise an exception
        processor._insert_batch(batch_data)
        
        # Verify executemany was called
        mock_cursor.executemany.assert_called_once()
    
    @patch('mmcif_gen.facilities.pdbe.CIFReader')
    @patch('mmcif_gen.facilities.pdbe.SqliteReader')
    def test_insert_batch_database_error(self, mock_sqlite, mock_cif):
        """Test batch insertion with database error."""
        # Mock the database cursor to raise an error
        mock_cursor = MagicMock()
        mock_cursor.executemany.side_effect = sqlite3.Error("Database error")
        mock_sqlite.return_value.sqlite_db_connection.return_value.__enter__.return_value = mock_cursor
        
        processor = InvestigationPdbe(
            ["/dummy/file.cif"],
            "test_investigation",
            self.temp_dir,
            self.operation_file
        )
        
        batch_data = [{
            "ordinal": 1,
            "pdb_id": "1ABC",
            "file_name": "test.cif",
            "model_file_no": "",
            "entity_id": "1",
            "type": "polymer",
            "seq_one_letter_code": "ACGT",
            "seq_one_letter_code_can": "ACGT",
            "chem_comp_id": "",
            "src_method": "nat",
            "description": "Test protein",
            "poly_type": "protein",
            "organism_scientific": "Test organism",
            "ncbi_taxonomy_id": "12345"
        }]
        
        with self.assertRaises(sqlite3.Error):
            processor._insert_batch(batch_data)


class TestDownloadFunctionality(unittest.TestCase):
    """Test cases for download functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        """Clean up test fixtures."""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    @patch('mmcif_gen.facilities.pdbe.requests.get')
    @patch('mmcif_gen.facilities.pdbe.run')
    @patch('mmcif_gen.facilities.pdbe.tempfile.mkdtemp')
    def test_download_success(self, mock_mkdtemp, mock_run, mock_get):
        """Test successful download and processing."""
        mock_mkdtemp.return_value = self.temp_dir
        
        # Mock successful HTTP response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b"fake gzipped content"
        mock_get.return_value = mock_response
        
        # Mock gzip decompression
        with patch('gzip.open', mock_open(read_data=b"data_1ABC\n")):
            with patch('builtins.open', mock_open()):
                download_and_run_pdbe_investigation(
                    ["1ABC"], "test_inv", self.temp_dir, "config.json"
                )
        
        # Verify run was called
        mock_run.assert_called_once()
    
    @patch('mmcif_gen.facilities.pdbe.requests.get')
    @patch('mmcif_gen.facilities.pdbe.tempfile.mkdtemp')
    def test_download_failure_all_sources(self, mock_mkdtemp, mock_get):
        """Test download failure from all sources."""
        mock_mkdtemp.return_value = self.temp_dir
        
        # Mock failed HTTP response
        mock_response = Mock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response
        
        with self.assertRaises(ValueError):
            download_and_run_pdbe_investigation(
                ["1ABC"], "test_inv", self.temp_dir, "config.json"
            )
    
    @patch('mmcif_gen.facilities.pdbe.requests.get')
    def test_download_invalid_pdb_ids(self, mock_get):
        """Test download with invalid PDB IDs."""
        with self.assertRaises(ValueError):
            download_and_run_pdbe_investigation(
                ["invalid_id"], "test_inv", self.temp_dir, "config.json"
            )


class TestFileUtilities(unittest.TestCase):
    """Test cases for file utility functions."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        """Clean up test fixtures."""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def test_get_cif_file_paths_success(self):
        """Test successful CIF file discovery."""
        # Create test CIF files
        cif_files = [
            os.path.join(self.temp_dir, "test1.cif"),
            os.path.join(self.temp_dir, "test2.cif"),
            os.path.join(self.temp_dir, "subdir", "test3.cif")
        ]
        
        os.makedirs(os.path.join(self.temp_dir, "subdir"))
        
        for file_path in cif_files:
            with open(file_path, 'w') as f:
                f.write("data_test\n")  # Valid mmCIF start
        
        # Create non-CIF file (should be ignored)
        with open(os.path.join(self.temp_dir, "test.txt"), 'w') as f:
            f.write("not a cif file")
        
        result = get_cif_file_paths(self.temp_dir)
        
        self.assertEqual(len(result), 3)
        for cif_file in cif_files:
            self.assertIn(cif_file, result)
    
    def test_get_cif_file_paths_no_files(self):
        """Test CIF file discovery with no valid files."""
        with self.assertRaises(ValueError):
            get_cif_file_paths(self.temp_dir)
    
    def test_get_cif_file_paths_nonexistent_directory(self):
        """Test CIF file discovery with non-existent directory."""
        with self.assertRaises(ValueError):
            get_cif_file_paths("/nonexistent/directory")
    
    def test_cleanup_temp_files(self):
        """Test temporary file cleanup."""
        # Create test files
        test_files = ["1ABC.cif", "1ABC.cif.gz", "2DEF.cif", "2DEF.cif.gz"]
        for filename in test_files:
            file_path = os.path.join(self.temp_dir, filename)
            with open(file_path, 'w') as f:
                f.write("test content")
        
        # Verify files exist before cleanup
        for filename in test_files:
            file_path = os.path.join(self.temp_dir, filename)
            self.assertTrue(os.path.exists(file_path))
        
        pdb_ids = ["1ABC", "2DEF"]
        cleanup_temp_files(self.temp_dir, pdb_ids)
        
        # The cleanup function removes the entire directory
        self.assertFalse(os.path.exists(self.temp_dir))


class TestCSVParsing(unittest.TestCase):
    """Test cases for CSV parsing functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.csv_file = os.path.join(self.temp_dir, "test.csv")
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir)
    
    def test_parse_csv_success(self):
        """Test successful CSV parsing."""
        csv_content = """GROUP_ID,ENTRY_ID
group1,1ABC
group1,2DEF
group2,3GHI
group2,4JKL
"""
        with open(self.csv_file, 'w') as f:
            f.write(csv_content)
        
        result = parse_csv(self.csv_file)
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result["group1"], ["1ABC", "2DEF"])
        self.assertEqual(result["group2"], ["3GHI", "4JKL"])
    
    def test_parse_csv_missing_columns(self):
        """Test CSV parsing with missing required columns."""
        csv_content = """WRONG_HEADER,ENTRY_ID
group1,1ABC
"""
        with open(self.csv_file, 'w') as f:
            f.write(csv_content)
        
        with self.assertRaises(ValueError):
            parse_csv(self.csv_file)
    
    def test_parse_csv_empty_values(self):
        """Test CSV parsing with empty values."""
        csv_content = """GROUP_ID,ENTRY_ID
group1,1ABC
,2DEF
group2,
"""
        with open(self.csv_file, 'w') as f:
            f.write(csv_content)
        
        result = parse_csv(self.csv_file)
        
        # Should only include valid entries
        self.assertEqual(len(result), 1)
        self.assertEqual(result["group1"], ["1ABC"])
    
    def test_parse_csv_nonexistent_file(self):
        """Test CSV parsing with non-existent file."""
        with self.assertRaises(ValueError):
            parse_csv("/nonexistent/file.csv")


class TestPerformance(unittest.TestCase):
    """Performance test cases."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.operation_file = os.path.join(self.temp_dir, "operation.json")
        
        with open(self.operation_file, 'w') as f:
            json.dump({"test": "data"}, f)
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir)
    
    @patch('mmcif_gen.facilities.pdbe.CIFReader')
    @patch('mmcif_gen.facilities.pdbe.SqliteReader')
    def test_batch_processing_performance(self, mock_sqlite, mock_cif):
        """Test that batch processing handles large datasets efficiently."""
        # Mock the database cursor
        mock_cursor = MagicMock()
        mock_sqlite.return_value.sqlite_db_connection.return_value.__enter__.return_value = mock_cursor
        
        # Test with small batch size to ensure batching works
        config = PdbeConfig(batch_size=10)
        processor = InvestigationPdbe(
            ["/dummy/file.cif"],
            "test_investigation",
            self.temp_dir,
            self.operation_file,
            config=config
        )
        
        # Create large batch of test data
        large_batch = []
        for i in range(25):  # 25 items with batch size 10 = 3 batches
            large_batch.append({
                "ordinal": i,
                "pdb_id": f"{i:04d}",
                "file_name": "test.cif",
                "model_file_no": "",
                "entity_id": str(i),
                "type": "polymer",
                "seq_one_letter_code": "ACGT",
                "seq_one_letter_code_can": "ACGT",
                "chem_comp_id": "",
                "src_method": "nat",
                "description": "Test protein",
                "poly_type": "protein",
                "organism_scientific": "Test organism",
                "ncbi_taxonomy_id": "12345"
            })
        
        processor.process_entities_in_batches(large_batch)
        
        # Should have called executemany 3 times (2 full batches + 1 partial)
        self.assertEqual(mock_cursor.executemany.call_count, 3)


class TestIntegration(unittest.TestCase):
    """Integration test cases."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.output_dir = os.path.join(self.temp_dir, "output")
        os.makedirs(self.output_dir)
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir)
    
    @patch('mmcif_gen.facilities.pdbe.get_cif_file_paths')
    @patch('mmcif_gen.facilities.pdbe.InvestigationPdbe')
    def test_run_function_integration(self, mock_processor_class, mock_get_files):
        """Test integration of run function."""
        # Mock file discovery
        mock_get_files.return_value = ["/path/to/test.cif"]
        
        # Mock processor
        mock_processor = Mock()
        mock_processor_class.return_value = mock_processor
        
        # Test the run function
        run(self.temp_dir, "test_inv", self.output_dir, "config.json")
        
        # Verify processor was created and methods called
        mock_processor_class.assert_called_once()
        mock_processor.pre_run.assert_called_once()
        mock_processor.run.assert_called_once()


if __name__ == '__main__':
    # Create test suite
    test_suite = unittest.TestSuite()
    
    # Add test classes
    test_classes = [
        TestPdbeConfig,
        TestPdbIdValidation,
        TestInvestigationPdbeInit,
        TestInputValidation,
        TestSafeExtractField,
        TestBatchProcessing,
        TestDownloadFunctionality,
        TestFileUtilities,
        TestCSVParsing,
        TestPerformance,
        TestIntegration
    ]
    
    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)
    
    # Run tests with detailed output
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # Print summary
    print(f"\n{'='*50}")
    print(f"Test Summary:")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Success rate: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.1f}%")
    print(f"{'='*50}")
