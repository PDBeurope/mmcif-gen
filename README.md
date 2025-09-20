# mmcif-gen

A versatile command-line tool for generating any mmCIF files from various data sources. This tool can be to create:

1. Metadata mmCIF files (To capture experimental metadata from different facilities)
2. Investigation mmCIF files (like: https://ftp.ebi.ac.uk/pub/databases/msd/fragment_screening/investigations/)

As is standard practice at the Protein Data Bank (PDB) the files generated are given the extension '.CIF' even though the file format is called mmCIF.
More on mmCIF file format can be found here: [mmcif.wwpdb.org/](https://mmcif.wwpdb.org/)

The tool has transformational mapping to convert data as it is stored at various facilities to corresponding catgories and items in mmcif format.

## Installation

### Basic Installation

Install directly from PyPI:

```bash
pip install mmcif-gen
```

### Dependencies

The tool requires several dependencies for full functionality:

#### Core Dependencies (automatically installed)
- `gemmi>=0.6.4` - mmCIF file parsing and processing
- `requests>=2.31.0` - HTTP requests for downloading PDB files
- `pyjq>=2.3.1` - JSON query processing

#### Optional Dependencies

For **performance testing and monitoring**:
```bash
pip install psutil>=5.9.0
```

For **chemical structure utilities** (SMILES to InChI conversion):
```bash
pip install openbabel>=3.1.0
```

#### Install All Optional Dependencies
```bash
# Install all optional dependencies at once
pip install -r requirements-optional.txt
```

#### Development Installation
```bash
# Clone the repository
git clone https://github.com/PDBeurope/mmcif-gen.git
cd mmcif-gen

# Install in development mode with all dependencies
pip install -e .
pip install -r requirements-optional.txt
```

## Usage

The tool provides two main commands:

1. `fetch-facility-json`: Fetch facility-specific JSON configuration files
2. `make-mmcif`: Generate mmCIF files using the configurations

### Fetching Facility JSON Files

The JSON operations files determine how the data would be mapped from the original source and translated into mmCIF format.

These files can be written, but can also be fetched from the github repository using simple commands.

```bash
# Fetch configuration for a specific facility
mmcif-gen fetch-facility-json dls-metadata

# Specify custom output directory
mmcif-gen fetch-facility-json dls-metadata -o ./mapping_operations
```

### Generating metadata mmCIF Files

Currently the valid facilities to generate mmcif files for are `pdbe`, `maxiv`, `dls`, and `xchem`.

The general syntax for generating mmCIF files is:

```bash
mmcif-gen make-mmcif <facility> [options]
````

Full list of options:
```
[w3_pdb05@pdb-001 Investigations]$ mmcif-gen make-mmcif --help
usage: mmcif-gen make-mmcif [-h] [--json JSON] [--output-folder OUTPUT_FOLDER]
                            [--id ID]
                            {pdbe,maxiv,dls,xchem} ...

positional arguments:
  {pdbe,maxiv,dls,xchem}
                        Specifies facility for which mmcif files will be used
                        for
    pdbe                Parameter requirements for investigation files from
                        PDBe data
    maxiv               Parameter requirements for investigation files from
                        MAX IV data
    dls                 Parameter requirements for creating investigation
                        files from DLS data
    xchem               Parameter requirements for creating investigation
                        files from XChem data

optional arguments:
  -h, --help            show this help message and exit
  --json JSON           Path to transformation JSON file
  --output-folder OUTPUT_FOLDER
                        Output folder for mmCIF files
  --id ID               File identifier
```

Each facility has its own set of required parameters, which can be checked by running the command with the `--help` flag.


```
mmcif-gen make-mmcif pdbe --help
```
#### Example Usage

#### DLS (Diamond Light Source)

```bash
# Using metadata configuration
mmcif-gen make-mmcif --json dls_metadata.json --output-folder ./out --id I_1234 dls --dls-json metadata-from-isypb.json
```

#### XChem
Parameters required
```
$ mmcif-gen make-mmcif xchem --help                                                                      
usage: mmcif-gen make-mmcif xchem [-h] [--sqlite SQLITE] [--cif-type {model,investigation}]

options:
  -h, --help            show this help message and exit
  --sqlite SQLITE       Path to the .sqlite file for each data set
  --cif-type {model,investigation}
                        Type of the CIF file that will be generated
```

Example command:
```
mmcif-gen make-mmcif --id 001 --json mmcif_gen/operations/xchem/xchem_metadata.json --output-folder pdbedeposit xchem --sqlite mmcif_gen/test/data/lb32633-1-soakDBDataFile.sqlite --cif-type model
```

### Working with Investigation Files

Investigation files are a specialized type of mmCIF file that capture metadata across multiple experiments.

Investigation files are created in a very similar way:

#### PDBe

```bash
# Using model folder
mmcif-gen make-mmcif --json pdbe_investigation.json --output-folder ./out --id I_1234 pdbe --model-folder ./models 

# Using PDB IDs
mmcif-gen make-mmcif  --json pdbe_investigation.json --output-folder ./out pdbe  --pdb-ids 6dmn 6dpp 6do8

# Using CSV input
mmcif-gen make-mmcif  --json pdbe_investigation.json --output-folder ./out pdbe --csv-file groups.csv 
```

#### MAX IV

```bash
# Using SQLite database
mmcif-gen make-mmcif maxiv --json maxiv_investigation.json --sqlite fragmax.sqlite --output-folder ./out --id I_1234
```

#### XChem

```bash
# Using SQLite database with additional information
mmcif-gen make-mmcif xchem --json xchem_investigation.json --sqlite soakdb.sqlite --txt ./metadata --deposit ./deposit --output-folder ./out
```


## Data Enrichment

For investigation files that need enrichment with additional data (e.g., ground state information):

```bash
# Using the miss_importer utility
python miss_importer.py --investigation-file inv.cif --sf-file structure.sf --pdb-id 1ABC
```

## Operation JSON Files

The tool uses JSON configuration files to define how data should be transformed into mmCIF format. These files can be:

1. Fetched files using the `fetch-facility-json` command
2. Modified versions of official configurations

### Configuration File Structure

```json
    {
        "source_category" : "_audit_author",
        "source_items" : ["name"],
        "target_category" : "_audit_author",
        "target_items" : "_same",
        "operation" : "distinct_union",
        "operation_parameters" :{
            "primary_parameters" : ["name"]
        }
    }
```

Refer to existing JSON files in the `operations/` directory for examples.


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

The project includes a comprehensive test suite with unit, integration, and performance tests:

```bash
# Run all tests
cd mmcif_gen/test/pdbe_tests
python run_tests.py --all --verbose

# Run specific test categories
python run_tests.py --unit          # Unit tests only
python run_tests.py --integration   # Integration tests only
python run_tests.py --performance   # Performance tests only

# Run individual test files
python -m unittest test_pdbe -v
python -m unittest test_pdbe_performance -v
```

**Note**: Performance tests require `psutil` for memory monitoring. Install with:
```bash
pip install psutil>=5.9.0
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.


## Support

For issues and questions, please use the [GitHub issue tracker](https://github.com/PDBeurope/Investigations/issues).
