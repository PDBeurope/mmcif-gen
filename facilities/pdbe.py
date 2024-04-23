from investigation_engine import InvestigationEngine
from investigation_io import CIFReader
import os
import requests
from typing import List, Dict
import sys
import logging
import gzip
import tempfile
import shutil
import csv


logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

FTP_URL_UPDATED = (
    "https://ftp.ebi.ac.uk/pub/databases/msd/updated_mmcif/divided/{}/{}_updated.cif.gz"
)
FTP_URL_ARCHIVE = (
    "https://ftp.ebi.ac.uk/pub/databases/pdb/data/structures/divided/mmCIF/{}/{}.cif.gz"
)

class InvestigationPdbe(InvestigationEngine):
        
    def __init__(self, model_file_path: List[str], investigation_id: str, output_path: str) -> None:
        logging.info("Instantiating PDBe Investigation subclass")
        self.reader = CIFReader()
        self.model_file_path = model_file_path
        super().__init__(investigation_id, output_path)

    def pre_run(self) -> None:
        logging.info("Pre-running")
        self.reader.read_files(self.model_file_path)
        self.reader.create_denormalised_tables()
        self.reader.build_denormalised_data()
        self.reader.add_struct_ref_data()
        self.reader.add_descript_categories()
        self.reader.add_sample_category()
        self.reader.add_synchrotron_data()
        self.reader.add_exptl_data()
        self.reader.add_investigation_id(self.investigation_id)
        super().pre_run()


def download_and_run_pdbe_investigation(pdb_ids: List[str], investigation_id: str, output_path:str) -> None:
    logging.info(f"Creating investigation files for pdb ids: {pdb_ids}")
    temp_dir = tempfile.mkdtemp()
    try:
        for pdb_code in pdb_ids:
            url = FTP_URL_ARCHIVE.format(pdb_code[1:3], pdb_code)

            compressed_file_path = os.path.join(temp_dir, f"{pdb_code}.cif.gz")
            uncompressed_file_path = os.path.join(temp_dir, f"{pdb_code}.cif")

            response = requests.get(url)
            if response.status_code == 200:
                with open(compressed_file_path, "wb") as f:
                    f.write(response.content)

                with gzip.open(compressed_file_path, "rb") as gz:
                    with open(uncompressed_file_path, "wb") as f:
                        f.write(gz.read())
                logging.info(f"Downloaded and unzipped {pdb_code}.cif")
            else:
                logging.info(f"Failed to download {pdb_code}.cif.gz")

        run(temp_dir, investigation_id, output_path)

    except Exception as e:
        logging.exception(f"An error occurred: {str(e)}")

    finally:
        for pdb_code in pdb_ids:
            compressed_file_path = os.path.join(temp_dir, f"{pdb_code}.cif.gz")
            uncompressed_file_path = os.path.join(temp_dir, f"{pdb_code}.cif")
            if os.path.exists(compressed_file_path):
                os.remove(compressed_file_path)
            if os.path.exists(uncompressed_file_path):
                os.remove(uncompressed_file_path)

        shutil.rmtree(temp_dir)

def run_investigation_pdbe(args):
    if args.model_folder:
        run(args.model_folder, args.investigation_id,args.output_folder)
    elif args.pdb_ids:
        download_and_run_pdbe_investigation(args.pdb_ids, args.investigation_id, args.output_folder)
    elif args.csv_file:
        group_data = parse_csv(args.csv_file)
        for group, entry in group_data.items():
            try:
                download_and_run_pdbe_investigation(entry, group, args.output_folder)
            except Exception as e:
                logging.exception(e)
    else:
        logging.error("PDBe Facilitiy requires parameter: --model-folder OR --csv-file OR --pdb-ids ")


def get_cif_file_paths(folder_path : str) -> List[str]:
    cif_file_paths = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if ".cif" in file and ".gz" not in file:
                cif_file_paths.append(os.path.join(root, file))
    if not cif_file_paths:
        logging.warn(f"No cif files in the folder path: {folder_path}")
        raise Exception("Model file path is empty")
    return cif_file_paths


def run(folder_path : str, investigation_id: str, output_path: str) -> None:
    model_file_path = get_cif_file_paths(folder_path)
    print("List of CIF file paths:")
    for file_path in model_file_path:
        print(file_path)
    im = InvestigationPdbe(model_file_path, investigation_id, output_path)
    im.pre_run()
    im.run()

def parse_csv(csv_file:str) -> Dict:
    group_data = {}
    with open(csv_file) as file:
        csv_reader = csv.DictReader(file, delimiter=",")
        for row in csv_reader:
            group_id = row["GROUP_ID"]
            entry_id = row["ENTRY_ID"]

            if group_id in group_data:
                group_data[group_id].append(entry_id)
            else:
                group_data[group_id] = [entry_id]
    return group_data
    
def pdbe_subparser(subparsers, parent_parser):
    parser_pdbe = subparsers.add_parser("pdbe",help="Parameter requirements for investigation files from PDBe data", parents=[parent_parser])
    parser_pdbe.add_argument(
        "-f", 
        "--model-folder", help="Directory which contains model files"
    )
    parser_pdbe.add_argument(
        "-csv", 
        "--csv-file", help="Requires CSV with 2 columns [GROUP_ID, ENTRY_ID]"
    )
    parser_pdbe.add_argument(
        "-p",
        "--pdb-ids",
        nargs="+",
        help="Create investigation from set of pdb ids, space seperated",
    )
