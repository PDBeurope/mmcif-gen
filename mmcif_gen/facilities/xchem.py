from mmcif_gen.investigation_engine import InvestigationEngine
from mmcif_gen.investigation_io import SqliteReader,CIFReader,PickleReader
from typing import List
import sys
import os
import logging

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

class InvestigationXChem(InvestigationEngine):
        
    def __init__(self, sqlite_path: str, deposit_path: str, cif_files: str, investigation_id: str, output_path: str) -> None:
        logging.info("Instantiating XChem Investigation subclass")
        self.sqlite_reader= SqliteReader(sqlite_path)
        self.pickle_reader = PickleReader(deposit_path).data
        self.reader = CIFReader()
        self.reader.read_files(cif_files)
        self.operation_file_json = "./operations/xchem_operations.json"
        self.excluded_libraries = ["'Diffraction Test'","'Solvent'"]
        super().__init__(investigation_id, output_path)

    def pre_run(self) -> None:
        logging.info("Pre-running")
        libraries=["XChem_Libraries_2024-02-01.csv"]
        for library in libraries:
            self.load_library_csv(f"./external_data/{library}")
        self.create_experiment_table()
        self.find_missing_compound_information()
        super().pre_run()

    def find_missing_compound_information(self) -> None:
        missing_compounds = self.sqlite_reader.sql_execute(f"SELECT DISTINCT b.CompoundCode, b.ID, b.LibraryName FROM mainTable b LEFT JOIN `XChem_Libraries_2024-02-01` a ON b.CompoundCode = a.vendor_catalog_ID WHERE a.vendor_catalog_ID IS NULL AND b.LibraryName NOT IN ({','.join(self.excluded_libraries)})")
        logging.warning(f"Number of missing compounds: {len(missing_compounds)}")
        for compound in missing_compounds:
            logging.warning(f"Compound Code: {compound[0]}, ID: {compound[1]}, Library Name: {compound[2]}")

    def create_experiment_table(self) -> None:
        # Retrieve distinct series based on the provided query
        distinct_series = self.sqlite_reader.sql_execute('''
            SELECT DISTINCT LibraryName 
            FROM mainTable 
            WHERE CompoundSMILES IS NOT NULL 
                AND CompoundSMILES != '' 
                AND CompoundSMILES != '-'
        ''')
        
        # Create a mapping from LibraryName to series_id
        series_mapping = {}
        alloted_id = 1
        for row in distinct_series:
            if row[0] in series_mapping:
                continue
            else:
                series_mapping[row[0]] = alloted_id
                alloted_id += 1
            
        logging.info(f"Series Mapping: {series_mapping}")
        
        experimental_data = self.sqlite_reader.sql_execute(f'''
            SELECT a.CompoundCode, a.LibraryName, a.CompoundSMILES, a.RefinementOutcome, b.`Parent InChI Key` 
            FROM mainTable a 
            INNER JOIN `XChem_Libraries_2024-02-01` b 
                ON a.CompoundCode = b.vendor_catalog_ID
            WHERE a.LibraryName NOT IN ({','.join(self.excluded_libraries)});
        ''')

        # inchi_keys_mapping = {inchi_key[4]: idx + 1 for idx, inchi_key in enumerate(experimental_data)}
        inchi_keys_mapping = {}
        alloted_id = 1
        for inchi_key in experimental_data:
            if inchi_key[4] in inchi_keys_mapping:
                continue
            else:
                inchi_keys_mapping[inchi_key[4]] = alloted_id
                alloted_id += 1

        # Drop the experiments table if it exists
        self.sqlite_reader.sql_execute("DROP TABLE IF EXISTS experiments")
        
        # Create the experiments table with series_id and series columns
        self.sqlite_reader.sql_execute('''
            CREATE TABLE experiments (
                screening_exp_id INTEGER PRIMARY KEY AUTOINCREMENT, 
                investigation_id TEXT,
                sample_id INTEGER,
                campaign_id TEXT,
                series_id INTEGER,
                series TEXT,
                library_name TEXT,
                compound_smiles TEXT,
                compound_code TEXT,
                fragment_component_mix_id INTEGER,
                result_id INTEGER,
                fraglib_component_id INTEGER,
                refinement_outcome TEXT,
                outcome TEXT,
                outcome_assessment TEXT,
                outcome_description TEXT,
                outcome_details TEXT,
                data_deposited TEXT
            )
        ''')
        
        with self.sqlite_reader.sqlite_db_connection() as cursor:
            for index, experiment in enumerate(experimental_data):
                library_name = experiment[1]
                inchi_key = experiment[4]
                refinement_outcome = experiment[3]

                
                # Determine outcome based on refinement_outcome
                outcome = None
                if refinement_outcome == '5 - Deposition ready':
                    outcome = 'hit'
                elif refinement_outcome == '7 - Analysed & Rejected':
                    outcome = 'miss'
                elif refinement_outcome == '4 - CompChem ready':
                    outcome = 'partial hit'
                
                # Determine outcome_assessment based on refinement_outcome
                if refinement_outcome == '5 - Deposition ready':
                    outcome_assessment = 'manual'
                elif refinement_outcome == '7 - Analysed & Rejected':
                    outcome_assessment = 'refined'
                else:
                    outcome_assessment = 'automatic'
                
                # Determine data_deposited based on refinement_outcome
                if refinement_outcome == '6 - Deposited':
                    data_deposited = 'Y'
                else:
                    data_deposited = 'N'
                
                # Set outcome_details only if 'Analysed & Rejected'
                outcome_details = 'Analysed & Rejected' if refinement_outcome == '7 - Analysed & Rejected' else None
                
                # Retrieve series_id from the mapping
                series_id = series_mapping.get(library_name)
                series = library_name  # Assuming series is equivalent to LibraryName
                fraglib_component_id = inchi_keys_mapping.get(inchi_key)
                
                if series_id is None:
                    logging.warning(f"LibraryName '{library_name}' not found in series mapping.")
                    continue  # Skip insertion if series_id is not found
                
                # Single insertion with all fields, including series_id and series
                cursor.execute('''
                    INSERT INTO experiments (
                        investigation_id, library_name, compound_smiles, compound_code, refinement_outcome,
                        campaign_id, sample_id, fraglib_component_id,
                        outcome, outcome_assessment, outcome_details,
                        series_id, series, data_deposited
                    ) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    self.investigation_id, 
                    library_name, 
                    experiment[2], 
                    experiment[0], 
                    refinement_outcome,
                    1,  # campaign_id
                    1,  # sample_id
                    fraglib_component_id,
                    outcome, 
                    outcome_assessment, 
                    outcome_details,
                    series_id,
                    series,
                    data_deposited
                ))
    
    def load_library_csv(self, csv_file: str) -> None:
        # Load csv file and put this on an sqlite table in the existing sqlite_reader
        self.sqlite_reader.create_table_from_csv(csv_file)

def get_cif_file_paths(folder_path : str) -> List[str]:
    cif_file_paths = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if ".txt" in file:
                cif_file_paths.append(os.path.join(root, file))
    if not cif_file_paths:
        logging.warn(f"No cif files in the folder path: {folder_path}")
        raise Exception("Model file path is empty")
    return cif_file_paths


def xchem_subparser(subparsers, parent_parser):
    parser_xchem = subparsers.add_parser("xchem", help="Parameter requirements for creating investigation files from XChem data", parents=[parent_parser])

    parser_xchem.add_argument(
        "--sqlite",
        help="Path to the .sqlite file for each data set"
    )
    parser_xchem.add_argument(
        "--deposit",
        help="Path for the deposition process via XCE"
    )
    parser_xchem.add_argument(
        "--txt",
        help="Path to add additional information or overwrite in mmcifs"
    )

def run(sqlite_path : str, deposit_path: str, txt_path: str, investigation_id: str, output_path: str) -> None:
    cif_files = []
    if txt_path:
        cif_files = get_cif_file_paths(txt_path)
        print("List of CIF file paths:")
        for file_path in cif_files:
            print(file_path)
    im = InvestigationXChem(sqlite_path, deposit_path, cif_files, investigation_id, output_path)
    im.pre_run()
    im.run()

def run_investigation_xchem(args):
    if not args.sqlite:
        logging.error("XChem facility requires path to --sqlite file")
        return 1
    run(args.sqlite, args.deposit, args.txt, args.investigation_id, args.output_folder)
