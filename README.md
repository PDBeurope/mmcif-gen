# mmcif-gen

A versatile command-line tool for generating mmCIF files from various facility data sources. This tool supports both generic mmCIF file generation and specialized investigation file creation for facilities like PDBe, MAX IV, XChem, and ESRF.

## Features

- Generate mmCIF files from various data sources (SQLite, JSON, CSV, etc.)
- Create standardized investigation files for facility data
- Support for multiple facilities (PDBe, MAX IV, ESRF, XChem)
- Configurable transformations via JSON definitions
- Auto-fetching of facility-specific configurations
- Modular design for easy extension to new data sources
- Data enrichment capabilities

## Installation

Install directly from PyPI:

```bash
pip install mmcif-gen
```

## Usage

The tool provides two main commands:

1. `fetch-facility-json`: Fetch facility-specific JSON configuration files
2. `make-mmcif`: Generate mmCIF files using the configurations

### Fetching Facility Configurations

```bash
# Fetch configuration for a specific facility
mmcif-gen fetch-facility-json dls-metadata

# Specify custom output directory
mmcif-gen fetch-facility-json dls-metadata -o ./configs
```

### Generating mmCIF Files

The general syntax for generating mmCIF files is:

```bash
mmcif-gen make-mmcif <facility> [options]
```

Each facility has its own set of required parameters:

#### PDBe

```bash
# Using model folder
mmcif-gen make-mmcif pdbe --model-folder ./models --output-folder ./out --identifier I_1234

# Using PDB IDs
mmcif-gen make-mmcif pdbe --pdb-ids 6dmn 6dpp 6do8 --output-folder ./out

# Using CSV input
mmcif-gen make-mmcif pdbe --csv-file groups.csv --output-folder ./out
```

#### MAX IV

```bash
# Using SQLite database
mmcif-gen make-mmcif maxiv --sqlite fragmax.sqlite --output-folder ./out --identifier I_5678
```

#### XChem

```bash
# Using SQLite database with additional information
mmcif-gen make-mmcif xchem --sqlite soakdb.sqlite --txt ./metadata --deposit ./deposit --output-folder ./out
```

#### DLS (Diamond Light Source)

```bash
# Using metadata configuration
mmcif-gen make-mmcif dls --json dls_metadata.json --output-folder ./out --identifier DLS_2024
```

## Configuration Files

The tool uses JSON configuration files to define how data should be transformed into mmCIF format. These files can be:

1. Fetched from the official repository using the `fetch-facility-json` command
2. Created custom for specific needs
3. Modified versions of official configurations

### Configuration File Structure

```json
{
  "source_category": "source_table_name",
  "target_category": "_target_category",
  "operations": [
    {
      "source_items": ["column1", "column2"],
      "target_items": ["_target.item1", "_target.item2"],
      "operation": "direct_transfer"
    }
  ]
}
```

## Working with Investigation Files

Investigation files are a specialized type of mmCIF file that capture metadata across multiple experiments. To create investigation files:

1. Use the appropriate facility subcommand
2. Specify the investigation ID
3. Provide the required facility-specific data source

```bash
# Example for PDBe investigation
mmcif-gen make-mmcif pdbe --model-folder ./models --identifier INV_001 --output-folder ./investigations

# Example for MAX IV investigation
mmcif-gen make-mmcif maxiv --sqlite experiment.sqlite --identifier INV_002 --output-folder ./investigations
```

## Data Enrichment

For investigation files that need enrichment with additional data (e.g., ground state information):

```bash
# Using the miss_importer utility
python miss_importer.py --investigation-file inv.cif --sf-file structure.sf --pdb-id 1ABC
```

## Development

### Project Structure

```
mmcif-gen/
├── facilities/            # Facility-specific implementations
│   ├── pdbe.py
│   ├── maxiv.py
│   └── ...
├── operations/           # JSON configuration files
│   ├── dls/
│   ├── maxiv/
│   └── ...
├── tests/               # Test cases
├── setup.py            # Package configuration
└── README.md          # Documentation
```

### Running Tests

```bash
python -m unittest discover -s tests
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

## License

[MIT License](LICENSE)

## Support

For issues and questions, please use the [GitHub issue tracker](https://github.com/PDBeurope/Investigations/issues).