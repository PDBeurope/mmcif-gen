from investigation_engine import InvestigationEngine
import argparse
import os
import logging
import sys
import os
import requests
import gzip
import tempfile
import shutil
import csv

FTP_URL_UPDATED = (
    "https://ftp.ebi.ac.uk/pub/databases/msd/updated_mmcif/divided/{}/{}_updated.cif.gz"
)
FTP_URL_ARCHIVE = (
    "https://ftp.ebi.ac.uk/pub/databases/pdb/data/structures/divided/mmCIF/{}/{}.cif.gz"
)
logging.basicConfig(stream=sys.stdout, encoding="utf-8", level=logging.INFO)

model_file_path = []


def download_and_create_investigation(pdb_ids, investigation_id):
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

        run_investigations(temp_dir, investigation_id)

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


def get_cif_file_paths(folder_path):
    cif_file_paths = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if ".cif" in file and ".gz" not in file:
                cif_file_paths.append(os.path.join(root, file))
    if not cif_file_paths:
        logging.warn(f"No cif files in the folder path: {folder_path}")
        raise Exception("Model file path is empty")
    return cif_file_paths


def run_investigations(folder_path, investigation_id):
    model_file_path = get_cif_file_paths(folder_path)
    print("List of CIF file paths:")
    for file_path in model_file_path:
        print(file_path)
    im = InvestigationEngine(model_file_path, investigation_id)
    im.pre_run()
    im.run()


def main():
    parser = argparse.ArgumentParser(
        prog="Investigation",
        description="This creates an investigation file from a collection of model files\
             which can be provided as folder path, pdb_ids, or a csv file. The model files can be provided",
    )
    parser.add_argument(
        "-m", "--model-folder", help="Directory which contains model files"
    )
    parser.add_argument(
        "-f", "--csv-file", help="Requires CSV with 2 columns [GROUP_ID, ENTRY_ID]"
    )
    parser.add_argument(
        "-p",
        "--pdb-ids",
        nargs="+",
        help="Create investigation from set of pdb ids, space seperated",
    )
    parser.add_argument(
        "-i",
        "--investigation-id",
        help="Investigation ID to assign to the resulting investigation file",
        default="I_1234",
    )

    args = parser.parse_args()

    if args.model_folder:
        run_investigations(args.model_folder, args.investigation_id)
    elif args.pdb_ids:
        download_and_create_investigation(args.pdb_ids, args.investigation_id)
    elif args.csv_file:
        group_data = {}
        with open(args.csv_file) as file:
            csv_reader = csv.DictReader(file, delimiter=",")
            for row in csv_reader:
                group_id = row["GROUP_ID"]
                entry_id = row["ENTRY_ID"]

                if group_id in group_data:
                    group_data[group_id].append(entry_id)
                else:
                    group_data[group_id] = [entry_id]
        print(group_data)
        for group, entry in group_data.items():
            try:
                download_and_create_investigation(entry, group)
            except Exception as e:
                logging.exception(e)


if __name__ == "__main__":
    main()
