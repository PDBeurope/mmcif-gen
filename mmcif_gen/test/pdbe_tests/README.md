# PDBe Investigation Facility - Test Suite

This directory contains a comprehensive test suite for the PDBe Investigation Facility (`pdbe.py`). The test suite is designed to ensure code quality, performance, and reliability across different scenarios.

## 📁 Test Structure

```
pdbe_tests/
├── pdbe_test_results/           # Folder for test output 
├── test_pdbe.py                 # Main unit and integration tests
├── test_pdbe_performance.py     # Performance and scalability tests
├── test_pdbe_mock_data.py       # Mock data generators and utilities
├── test_config.json             # Test configuration file
├── run_tests.py                 # Unified test runner
└── README.md                    # This file
```

## 🧪 Test Categories

### 1. Unit Tests (`test_pdbe.py`)

- **Configuration Management**: Test `PdbeConfig` class functionality
- **Input Validation**: Test PDB ID validation and file validation
- **Data Processing**: Test safe field extraction and data cleaning
- **Batch Processing**: Test database batch operations
- **File Utilities**: Test CIF file discovery and CSV parsing
- **Error Handling**: Test error recovery and edge cases

### 2. Integration Tests (`test_pdbe.py`)

- **Download Functionality**: Test PDB file download with fallback URLs
- **End-to-End Processing**: Test complete workflow from files to database
- **Configuration Integration**: Test configuration loading and application
- **Cleanup Operations**: Test temporary file cleanup

### 3. Performance Tests (`test_pdbe_performance.py`)

- **Batch Processing Performance**: Test processing speed with different batch sizes
- **Memory Usage Patterns**: Test memory efficiency and leak detection
- **Scalability Limits**: Test handling of large datasets
- **Concurrency Testing**: Test thread safety and concurrent operations

### 4. Mock Data Generation (`test_pdbe_mock_data.py`)

- **mmCIF File Generation**: Create realistic test mmCIF files
- **Database Record Generation**: Generate test entity records
- **Test Environment Setup**: Create complete test environments
- **Response Mocking**: Mock HTTP responses for download testing

## 🚀 Quick Start

### Prerequisites

```bash
# Install required dependencies
pip install psutil  # For memory monitoring (optional)
```

### Running Tests

#### Run All Tests

```bash
cd mmcif_gen/test/pdbe_tests
python run_tests.py --all --verbose
```

#### Run Specific Test Categories

```bash
cd mmcif_gen/test/pdbe_tests

# Unit tests only
python run_tests.py --unit

# Performance tests only
python run_tests.py --performance

# Integration tests only
python run_tests.py --integration
```

#### Run Individual Test Files

```bash
cd mmcif_gen/test/pdbe_tests

# Run main test suite
python -m unittest test_pdbe -v

# Run performance tests
python -m unittest test_pdbe_performance -v

# Run specific test class
python -m unittest test_pdbe.TestPdbIdValidation -v
```

## ⚙️ Configuration

### Test Configuration (`test_config.json`)

```json
{
  "test_configuration": {
    "unit_tests": {"enabled": true, "timeout": 30},
    "integration_tests": {"enabled": true, "timeout": 120},
    "performance_tests": {"enabled": true, "timeout": 300},
    "stress_tests": {"enabled": false, "timeout": 600}
  },
  "test_data": {
    "sample_pdb_ids": ["1ABC", "2DEF", "3GHI"],
    "test_batch_sizes": [1, 10, 100, 1000]
  },
  "reporting": {
    "generate_html_report": true,
    "cleanup_after_tests": true
  }
}
```

### Custom Configuration

```bash
python tests/run_tests.py --config my_test_config.json
```

## 📊 Test Reports

### HTML Reports

When enabled, HTML reports are generated in `test_results/reports/test_report.html` with:

- Test summary statistics
- Success/failure rates
- Detailed failure information
- Performance metrics

### Console Output

```
==========================================
Unit Tests
==========================================
test_valid_pdb_ids ... OK
test_invalid_pdb_ids ... OK
test_batch_processing ... OK

Performance Test Summary:
Tests run: 25
Failures: 0
Errors: 0
Success rate: 100.0%
```

## 🎯 Test Coverage

### Core Functionality

- ✅ PDB ID validation (valid/invalid formats)
- ✅ Configuration management (default/custom/file-based)
- ✅ Input validation (files, directories, parameters)
- ✅ Safe data extraction with error handling
- ✅ Batch processing with configurable sizes
- ✅ Database operations with error recovery
- ✅ File discovery and validation
- ✅ CSV parsing with validation
- ✅ Download functionality with fallbacks
- ✅ Cleanup operations

### Performance Aspects

- ✅ Memory usage monitoring
- ✅ Processing speed benchmarks
- ✅ Scalability testing (small to large datasets)
- ✅ Batch size optimization
- ✅ Memory leak detection
- ✅ Concurrent operation testing

### Error Scenarios

- ✅ Missing files and directories
- ✅ Malformed mmCIF data
- ✅ Network failures
- ✅ Database errors
- ✅ Invalid input data
- ✅ Resource exhaustion

## 🔧 Writing New Tests

### Unit Test Example

```python
class TestNewFeature(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
  
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir)
  
    def test_new_functionality(self):
        """Test new functionality."""
        # Arrange
        test_data = "test input"
    
        # Act
        result = new_function(test_data)
    
        # Assert
        self.assertEqual(result, "expected output")
```

### Performance Test Example

```python
class TestNewPerformance(PerformanceTestBase):
    def test_performance_metric(self):
        """Test performance of new feature."""
        start_time = time.time()
        start_memory = self.get_memory_usage()
    
        # Execute operation
        result = expensive_operation()
    
        end_time = time.time()
        end_memory = self.get_memory_usage()
    
        # Assert performance requirements
        execution_time = end_time - start_time
        memory_usage = end_memory - start_memory
    
        self.assertLess(execution_time, 1.0, "Operation too slow")
        self.assertLess(memory_usage, 10.0, "Memory usage too high")
```

## 🐛 Debugging Tests

### Verbose Output

```bash
python tests/run_tests.py --verbose
```

### Individual Test Debugging

```bash
python -m unittest tests.test_pdbe.TestSpecificFeature.test_specific_case -v
```

### Test Artifacts

Test artifacts are saved in `test_results/artifacts/` including:

- Temporary files created during tests
- Mock data generated
- Log files
- Performance metrics

## 📈 Performance Benchmarks

### Expected Performance Metrics

- **File Discovery**: < 1 second for 100 files
- **Batch Processing**: > 100 entities/second
- **Memory Usage**: < 10MB growth per 1000 entities
- **Database Operations**: < 0.1 seconds per batch

### Performance Test Categories

1. **Batch Size Optimization**: Test different batch sizes (1, 10, 100, 1000, 10000)
2. **Memory Efficiency**: Monitor memory usage patterns
3. **Scalability**: Test with datasets of varying sizes
4. **Concurrent Operations**: Test thread safety and parallel processing

## 🚨 Continuous Integration

### CI/CD Integration

```yaml
# Example GitHub Actions workflow
- name: Run Tests
  run: |
    python tests/run_tests.py --all
  
- name: Upload Test Reports
  uses: actions/upload-artifact@v2
  with:
    name: test-reports
    path: test_results/
```

### Test Quality Gates

- All unit tests must pass
- Integration tests must pass
- Performance tests must meet benchmarks
- Memory usage must stay within limits
- No memory leaks detected

## 📝 Best Practices

### Test Writing Guidelines

1. **Arrange-Act-Assert**: Structure tests clearly
2. **Descriptive Names**: Use descriptive test method names
3. **Single Responsibility**: Test one thing per test method
4. **Proper Cleanup**: Always clean up resources
5. **Mock External Dependencies**: Use mocks for external services
6. **Performance Assertions**: Include performance requirements

### Mock Data Guidelines

1. **Realistic Data**: Generate realistic test data
2. **Edge Cases**: Include edge cases and error conditions
3. **Variety**: Test with different data types and sizes
4. **Consistency**: Maintain consistent test data structure

## 🔍 Troubleshooting

### Common Issues

1. **Import Errors**: Ensure project root is in Python path
2. **Permission Errors**: Check file/directory permissions
3. **Memory Issues**: Reduce test dataset sizes
4. **Timeout Issues**: Increase timeout values in config

### Debug Mode

```bash
# Enable debug logging
export PYTHONPATH=.
python tests/run_tests.py --verbose --config debug_config.json
```

## 📚 Additional Resources

- [Python unittest documentation](https://docs.python.org/3/library/unittest.html)
- [Mock object library](https://docs.python.org/3/library/unittest.mock.html)
- [Performance testing best practices](https://docs.python.org/3/library/profile.html)

## 🤝 Contributing

When adding new features to `pdbe.py`:

1. Add corresponding unit tests
2. Add integration tests if needed
3. Add performance tests for new algorithms
4. Update mock data generators if needed
5. Run full test suite before submitting
6. Update this README if adding new test categories
