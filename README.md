# Investigations

## Project Description

The Investigations project is designed to facilitate the processing and analysis of data from various facilities, including PDBe, MAX IV, DLS and ESRF.
 The project provides a set of tools for managing and executing operations, importing data, and generating output in a mmCIF standardized format.

## Features

- Integration with multiple facilities (PDBe, MAX IV, ESRF, DLS)
- Comprehensive data import and export functionalities
- Modular design for easy extension and maintenance
- Robust error handling and logging
- Comprehensive test suite for ensuring code quality

## Installation

To get started with the Investigations project, follow these steps:

1. Clone the repository:

    ```bash
    git clone https://github.com/yourusername/investigations.git
    cd investigations
    ```

2. Create a virtual environment and activate it:

    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

3. Install the required dependencies:

    ```bash
    pip install -r requirements.txt
    ```

## Usage

Below are some common usage examples to help you get started with the Investigations project.

The script requires to specify a facility as the first argument:

```
python investigation.py --help
usage: Investigation [-h] {pdbe,max_iv,esrf} ...

This creates an investigation file from a collection of model files which can be provided as folder path, pdb_ids, or a csv file. The model files can be provided

positional arguments:
  {pdbe,max_iv,esrf}  Specifies facility for which investigation files will be used for
    pdbe              Parameter requirements for investigation files from PDBe data
    max_iv            Parameter requirements for investigation files from MAX IV data
    esrf              Parameter requirements for investigation files from ESRF data
    dls               Parameter requirements for investigation files from DLS data
```

Each facility have its own set of arguments. 
### For MAX IV
SqliteDB file is required

```
python investigation.py max_iv --help
usage: Investigation max_iv [-h] [-o OUTPUT_FOLDER] [-i INVESTIGATION_ID] [-s SQLITE]

optional arguments:
  -h, --help            show this help message and exit
  -o OUTPUT_FOLDER, --output-folder OUTPUT_FOLDER
                        Folder to output the created investigation files to
  -i INVESTIGATION_ID, --investigation-id INVESTIGATION_ID
                        Investigation ID to assign to the resulting investigation file
  -s SQLITE, --sqlite SQLITE
                        Path to the Sqlite DB for the given investigation
```

### For PDBE

The model files can be provided as a folder path, or as PDB Ids.
Where PDB ids are specified, the data is fetched from FTP Area of EBI PDB archive

```
python investigation.py pdbe --help  
usage: Investigation pdbe [-h] [-o OUTPUT_FOLDER] [-i INVESTIGATION_ID] [-f MODEL_FOLDER] [-csv CSV_FILE] [-p PDB_IDS [PDB_IDS ...]]

optional arguments:
  -h, --help            show this help message and exit
  -o OUTPUT_FOLDER, --output-folder OUTPUT_FOLDER
                        Folder to output the created investigation files to
  -i INVESTIGATION_ID, --investigation-id INVESTIGATION_ID
                        Investigation ID to assign to the resulting investigation file
  -f MODEL_FOLDER, --model-folder MODEL_FOLDER
                        Directory which contains model files
  -csv CSV_FILE, --csv-file CSV_FILE
                        Requires CSV with 2 columns [GROUP_ID, ENTRY_ID]
  -p PDB_IDS [PDB_IDS ...], --pdb-ids PDB_IDS [PDB_IDS ...]
                        Create investigation from set of pdb ids, space seperated
```
`--investigation-id` parameter is an optional parameter where the user wants to control the investigation ID that is assigned to the investigation file. It is not used where input is csv file. 


### For DLS

```
python investigation.py dls --help
usage: Investigation dls [-h] [-o OUTPUT_FOLDER] [-i INVESTIGATION_ID] [--sqlite SQLITE] [--deposit DEPOSIT] [--txt TXT]

optional arguments:
  -h, --help            show this help message and exit
  -o OUTPUT_FOLDER, --output-folder OUTPUT_FOLDER
                        Folder to output the created investigation files to
  -i INVESTIGATION_ID, --investigation-id INVESTIGATION_ID
                        Investigation ID to assign to the resulting investigation file
  --sqlite SQLITE       Path to the .sqlite file for each data set
  --deposit DEPOSIT     Path for the deposition process via XCE
  --txt TXT             Path to add additional information or overwrite in mmcifs
```
There are two operations defined for DLS facility.
dls_operations_soakdb.json: This operation file is used to create an investigation file from purely soakdb sqlite file. Data not found is highlighted in the output file
dls_operations.json: This operation file is used to create an investigation file and relies on pickle + cif files.


#### Importing data from Ground state file
For files where the data of misses are present in structure factor file, the `miss_importer.py` utility can be used to enrich the investigation data with new information.

```
$ python miss_importer.py --help
usage: Ground state file importer  [-h] [-inv INVESTIGATION_FILE] [-sf SF_FILE] [-p PDB_ID] [-f CSV_FILE]

This utility takes as an input investigation file, and sf file. And imports the data for all the misses from the sf file and adds that to the investigation file

optional arguments:
  -h, --help            show this help message and exit
  -inv INVESTIGATION_FILE, --investigation-file INVESTIGATION_FILE
                        Path to investigation file
  -sf SF_FILE, --sf-file SF_FILE
                        Path to structure factor file
  -p PDB_ID, --pdb-id PDB_ID
                        PDB ID to lookup to download the sf file
  -f CSV_FILE, --csv-file CSV_FILE
                        Requires CSV with 2 columns [investigation_file, Pdb Code (to fetch sf file)]
```

The utility requires the created investigation file, along with a sf file (or pdb code to automatically fetch the sf file) as input. 
And outputs a modified investigation cif file.

### Example

#### MAX IV

```
investigation.py max_iv --sqlite fragmax.sqlite -i inv_01
```

#### DLS
```
python investigation.py dls --sqlite DLS_data_example/soakDBDataFile_CHIKV_Mac.sqlite --txt DLS_data_example/ --deposit DLS_data_example/deposition.deposit -i inv_01 -o out/
```

#### PDBE
PDB Ids can be passed in the arguments. The model file is fetched from EBI Archive FTP area temporarily stored. After the investigation file is created the files are deleted.
```
python investigations.py pdbe -p 6dmn 6dpp 6do8
```

A path can be given to the application. All cif model files in the folder are regarded as input.
```
python investigations.py pdbe -m path/to/folder/with/model_files
```

A CSV file can be provided as input. The csv file should have two columns `GROUP_ID` and `ENTRY_ID`.
Entries in the same groups are processed together, and an investigation file is created for each unique `GROUP_ID`
```
python investigations.py pdbe -f path/to/csv/file
```

## Running Tests

To run the test suite, use the following command:

```bash
python -m unittest discover -s test
```

## Project Structure

- `investigation.py`: Core logic for handling investigations.
- `investigation_engine.py`: Manages the processing logic for investigations.
- `investigation_io.py`: Handles input/output operations for investigations.
- `operations.py`: Contains operational logic for various tasks.
- `util/`: Contains utility scripts.
- `facilities/`: Handles different facilities' operations.
- `test/`: Contains test cases and data for unit testing.
- `requirements.txt`: Lists the dependencies required for the project.
- `README.md`: Project documentation.

## Configuration

Configuration files for the operations can be found in the operations folder:

  - `pdbe_operations.json`
  - `maxiv_operations.json`
  - `dls_operations.json`
  - `dls_operations_soakdb.json`

These files contain necessary configurations for interacting with the respective facilities.


### Working
The investigation file is created from the constituent model file. The data from the model file is parsed via Gemmi and stored in a in-memory SQLite database, which denormalises the data in the various categories amongst all the files.

The operations.json file is read by the program, and operations specified are ran sequentially. 
The operations generally specify source and target category and items, operation to perform, and parameter that the operation may require.
The operation may leverage the denormalised table created initially.

Once all operations are peformed the resultant file is written out where name of the file is the investigation_id.
Incase an operation cannot be performed due to missing data in the file, the operation gets skipped and the error is logged.
