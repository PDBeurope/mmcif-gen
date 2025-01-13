# mmcif-gen

A versatile command-line tool for generating any mmCIF files from various data sources. This tool can be to create:

1. Metadata mmcif files (To capture experimental metadata from different facilities)
2. Investigation mmcif files (like: https://ftp.ebi.ac.uk/pub/databases/msd/fragment_screening/investigations/)

The tool has transformational mapping to convert data as it is stored at various facilities to corresponding catgories and items in mmcif format.

## Installation

Install directly from PyPI:

```bash
pip install mmcif-gen
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

```bash
python -m unittest discover -s tests
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.


## Support

For issues and questions, please use the [GitHub issue tracker](https://github.com/PDBeurope/Investigations/issues).