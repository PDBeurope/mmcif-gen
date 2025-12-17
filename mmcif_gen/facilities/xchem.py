from mmcif_gen.investigation_engine import InvestigationEngine
from mmcif_gen.investigation_io import SqliteReader,CSVReader
from typing import List, Tuple
from mmcif_gen.util.output_grabber import OutputGrabber
import sys
import os
import logging
import json
from enum import Enum
from rdkit import Chem
from rdkit.Chem import Descriptors

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

class CifType(Enum):
    Investigation = "Investigation"
    Model = "Model"

class CifXChem(InvestigationEngine):
        
    def __init__(self, investigation_id: str, sqlite_path: str, data_csv: str, output_path: str, json_path: str, cif_type: CifType) -> None:
        logging.info(f"Instantiating XChem CIF subclass - {cif_type.value}")
        self.sqlite_reader = SqliteReader(sqlite_path, use_temp_copy=True)
        self.data_csv = data_csv
        self.operation_file_json = json_path
        self.excluded_libraries = ["'Diffraction Test'","'Solvent'"]
        self.cif_type = cif_type
        super().__init__(investigation_id, output_path)

    def pre_run(self) -> None:
        logging.info("Pre-running")
        if self.cif_type == CifType.Investigation:
            self.sqlite_reader.create_mmcif_tables_from_csv(self.data_csv)
            self.create_experiment_table()

        super().pre_run()

    def read_json_operations(self) -> None:
        '''This is called by pre run in base class'''
        logging.info("Reading JSON operation files")
        with open(self.operation_file_json, "r") as file:
            json_data = json.load(file)
            if self.cif_type == CifType.Investigation:
                self.operations = json_data.get("investigation", []).get("operations", [])
                self.investigation_storage.mmcif_order = json_data.get("investigation", []).get("mmcif_order", [])
            elif self.cif_type == CifType.Model:
                self.operations = json_data.get("model", []).get("operations", [])
                self.investigation_storage.mmcif_order = json_data.get("model", []).get("mmcif_order", [])
            else:
                raise ValueError(f"Invalid CIF type: {self.cif_type}")

    def get_output_file_name(self) -> str:
        if self.cif_type == CifType.Investigation:
            return f"{self.output_path}/{self.investigation_id}.cif"
        else:
            return f"{self.output_path}/{self.investigation_id}_model.cif"

    def get_experimental_data(self) -> List[Tuple[str, str, str, str, str]]:
        experimental_data = self.sqlite_reader.sql_execute(f'''
            SELECT DISTINCT a.CompoundCode, a.LibraryName, a.CompoundSMILES, a.RefinementOutcome, "detail_placeholder" as CompoundDetails 
            FROM mainTable a 
            WHERE a.CompoundSMILES is not null and a.LibraryName NOT IN ({','.join(self.excluded_libraries)});
        ''')

        compound_details = {}
        out_stderr = OutputGrabber(sys.stderr)
        with out_stderr:
            try:
                for smile_string in experimental_data:
                    mol = Chem.MolFromSmiles(smile_string[2])
                    inchi = Chem.MolToInchi(mol)
                    inchi_key = Chem.InchiToInchiKey(inchi)
                    compound_details[smile_string[0]] = {
                        "inchi_key": inchi_key,
                        "formula": Chem.rdMolDescriptors.CalcMolFormula(mol),
                        "mol_weight": Descriptors.MolWt(mol)
                    }
            except Exception as e:
                logging.error("Conversion failure")

        updated_data = []
        for row in experimental_data:
            row = list(row)  # convert tuple to list
            compound_code = row[0]  # assuming index 0 = CompoundCode
            details = compound_details.get(compound_code, "")
            row[4] = details  # replace placeholder (index 4)
            updated_data.append(tuple(row)) 

        return updated_data
    
    def create_experiment_table(self) -> None:
        experimental_data = self.get_experimental_data()

        # inchi_keys_mapping = {inchi_key[4]: idx + 1 for idx, inchi_key in enumerate(experimental_data)}
        inchi_keys_mapping = {}
        alloted_id = 1
        for inchi_key in experimental_data:
            if inchi_key[4]["inchi_key"] in inchi_keys_mapping:
                continue
            else:
                inchi_keys_mapping[inchi_key[4]["inchi_key"]] = alloted_id
                alloted_id += 1
        
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
                inchi_key TEXT,
                formula TEXT,
                mol_weight TEXT,
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
            
        logging.info(f"Series Mapping: {series_mapping}")
        
        with self.sqlite_reader.sqlite_db_connection() as cursor:
            for index, experiment in enumerate(experimental_data):
                library_name = experiment[1]
                inchi_key = experiment[4]["inchi_key"]
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
                    # logging.warning(f"LibraryName '{library_name}' not found in series mapping.")
                    continue  # Skip insertion if series_id is not found
                
                # Single insertion with all fields, including series_id and series
                cursor.execute('''
                    INSERT INTO experiments (
                        investigation_id, library_name, compound_smiles, compound_code, refinement_outcome,
                        campaign_id, sample_id, fraglib_component_id, inchi_key, formula, mol_weight,
                        outcome, outcome_assessment, outcome_details,
                        series_id, series, data_deposited
                    ) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    self.investigation_id, 
                    library_name, 
                    experiment[2], 
                    experiment[0], 
                    refinement_outcome,
                    1,  # campaign_id
                    1,  # sample_id
                    fraglib_component_id,
                    experiment[4]["inchi_key"],
                    experiment[4]["formula"],
                    experiment[4]["mol_weight"],
                    outcome, 
                    outcome_assessment, 
                    outcome_details,
                    series_id,
                    series,
                    data_deposited
                ))

def get_cif_file_paths(folder_path : str) -> List[str]:
    cif_file_paths = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if ".txt" in file:
                cif_file_paths.append(os.path.join(root, file))
    if not cif_file_paths:
        logging.warning(f"No cif files in the folder path: {folder_path}")
        raise Exception("Model file path is empty")
    return cif_file_paths


def xchem_subparser(subparsers, parent_parser):
    parser_xchem = subparsers.add_parser("xchem", help="Parameter requirements for creating investigation files from XChem data", parents=[parent_parser])

    parser_xchem.add_argument(
        "--sqlite",
        help="Path to the .sqlite file for each data set"
    )
    parser_xchem.add_argument(
        "--data-csv",
        help="Path to the .csv file for each data set"
    )

def run(investigation_id: str, sqlite_path: str, data_csv: str,  output_path: str, json_path: str) -> None:
    investigation = CifXChem(investigation_id, sqlite_path, data_csv, output_path, json_path, cif_type=CifType.Investigation)
    investigation.pre_run()
    investigation.run()
    model = CifXChem(investigation_id, sqlite_path, data_csv, output_path, json_path, cif_type=CifType.Model)
    model.pre_run()
    model.run()

def run_investigation_xchem(args):
    if not args.sqlite or not args.data_csv :
        logging.error("XChem facility requires path to --sqlite file, --data-csv")
        return 1
    run(args.id, args.sqlite, args.data_csv, args.output_folder, args.json)
