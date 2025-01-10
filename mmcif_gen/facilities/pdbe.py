from mmcif_gen.investigation_engine import InvestigationEngine
from mmcif_gen.investigation_io import CIFReader, SqliteReader
import os
import requests
from typing import List, Dict
import sys
import logging
import gzip
import tempfile
import shutil
import csv
from contextlib import contextmanager
import sqlite3

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

FTP_URL_UPDATED = (
    "https://ftp.ebi.ac.uk/pub/databases/msd/updated_mmcif/divided/{}/{}_updated.cif.gz"
)
FTP_URL_ARCHIVE = (
    "https://ftp.ebi.ac.uk/pub/databases/pdb/data/structures/divided/mmCIF/{}/{}.cif.gz"
)

class InvestigationPdbe(InvestigationEngine):
        
    def __init__(self, model_file_path: List[str], investigation_id: str, output_path: str, pdbe_investigation_json: str="./operations/pdbe/pdbe_investigation.json") -> None:
        logging.info("Instantiating PDBe Investigation subclass")
        self.reader = CIFReader()
        self.model_file_path = model_file_path
        self.operation_file_json = "./operations/pdbe/pdbe_investigation.json"
        self.sqlite_reader = SqliteReader("pdbe_sqlite.db")
        super().__init__(investigation_id, output_path)

    def pre_run(self) -> None:
        logging.info("Pre-running")
        self.reader.read_files(self.model_file_path)
        self.create_denormalised_tables()
        self.build_denormalised_data()
        self.add_struct_ref_data()
        self.add_descript_categories()
        self.add_sample_category()
        self.add_synchrotron_data()
        self.add_exptl_data()
        self.add_investigation_id(self.investigation_id)
        super().pre_run()


    def sql_execute(self, query):
        logging.debug(f"Executing query: {query}")
        result = []
        with self.sqlite_reader.sqlite_db_connection() as conn:
            response = conn.execute(query)
            for row in response:
                result.append(row)
        return result

    def create_denormalised_tables(self):
        logging.info("Creating denormalised table")
        drop_denormalized_table = "DROP TABLE IF EXISTS denormalized_data;"
        create_denormalized_table = """
            CREATE TABLE denormalized_data (
                investigation_entity_id INT,
                pdb_id TEXT,
                model_file_no TEXT,
                file_name TEXT,
                entity_id TEXT,
                type TEXT,
                seq_one_letter_code TEXT,
                chem_comp_id TEXT,
                src_method TEXT,
                description TEXT,
                poly_type TEXT,
                poly_descript INT,
                nonpoly_descript INT,
                sample_id INT,
                db_name TEXT,
                db_code TEXT,
                db_accession TEXT,
                synchrotron_site TEXT,
                exptl_method TEXT,
                campaign_id TEXT,
                series_id TEXT,
                investigation_id TEXT
            )
        """
        with self.sqlite_reader.sqlite_db_connection() as cursor:
            cursor.execute(drop_denormalized_table)
            cursor.execute(create_denormalized_table)

    def build_denormalised_data(self):
        logging.info("Building Denormalized data table from the cif files")
        denormalized_data = []
        ordinals = {}
        next_poly_ordinal = 1
        next_nonpoly_ordinal = 1
        for file_name, datablock in self.reader.data.items():
            entity_category = datablock.find_mmcif_category("_entity")
            entity_poly_category = datablock.find_mmcif_category("_entity_poly")
            entity_nonpoly_category = datablock.find_mmcif_category(
                "_pdbx_entity_nonpoly"
            )
            database_2_category = datablock.find_mmcif_category("_database_2")

            # Create dictionaries to map column names to their indices
            entity_columns = {name: i for i, name in enumerate(entity_category.tags)}
            poly_columns = {name: i for i, name in enumerate(entity_poly_category.tags)}
            nonpoly_columns = {
                name: i for i, name in enumerate(entity_nonpoly_category.tags)
            }
            database_2_columns = {
                name: i for i, name in enumerate(database_2_category.tags)
            }

            pdb_id = database_2_category[0][
                database_2_columns["_database_2.database_code"]
            ]
            if entity_category is not None:
                for row in entity_category:
                    entity_id = row[entity_columns["_entity.id"]]
                    entity_type = row[entity_columns["_entity.type"]]
                    src_method = row[entity_columns["_entity.src_method"]]
                    description = row[entity_columns["_entity.pdbx_description"]].strip("'").strip(";").strip("\n")
                    chem_comp_id = ""
                    seq_one_letter_code = ""
                    ordinal = ""

                    if entity_type == "polymer":
                        seq_one_letter_code = ""
                        poly_type = ""
                        # Check if the entity has polymer data
                        if entity_poly_category is not None:
                            for poly_row in entity_poly_category:
                                if (
                                    poly_row[poly_columns["_entity_poly.entity_id"]]
                                    == entity_id
                                ):
                                    seq_one_letter_code = poly_row[
                                        poly_columns[
                                            "_entity_poly.pdbx_seq_one_letter_code"
                                        ]
                                    ]
                                    poly_type = poly_row[
                                        poly_columns["_entity_poly.type"]
                                    ]

                        ordinal = ordinals.get(seq_one_letter_code, False)
                        if not ordinal:
                            ordinal = next_poly_ordinal
                            ordinals[seq_one_letter_code] = next_poly_ordinal
                            next_poly_ordinal = next_poly_ordinal + 1

                    elif entity_type in ["water", "non-polymer"]:
                        # Check if the entity has non-polymer data
                        if entity_nonpoly_category is not None:
                            for nonpoly_row in entity_nonpoly_category:
                                if (
                                    nonpoly_row[
                                        nonpoly_columns[
                                            "_pdbx_entity_nonpoly.entity_id"
                                        ]
                                    ]
                                    == entity_id
                                ):
                                    chem_comp_id = nonpoly_row[
                                        nonpoly_columns["_pdbx_entity_nonpoly.comp_id"]
                                    ]
                        ordinal = ordinals.get(chem_comp_id, False)
                        if not ordinal:
                            ordinal = next_nonpoly_ordinal
                            ordinals[chem_comp_id] = ordinal
                            next_nonpoly_ordinal = next_nonpoly_ordinal + 1

                    denormalized_data.append(
                        {
                            "ordinal": ordinal,
                            "pdb_id": pdb_id,
                            "file_name": file_name,
                            "model_file_no": "",  
                            "entity_id": entity_id,
                            "type": entity_type,
                            "seq_one_letter_code": seq_one_letter_code.strip(";").rstrip('\n'),  # Placeholder for polymer data
                            "chem_comp_id": chem_comp_id,
                            "src_method": src_method,
                            "poly_type": poly_type.strip("'"),
                            "description": description,
                        }
                    )
        logging.info("Successfully built the data for the table")
        logging.info("Loading table into In-memory Sqlite")

        with self.sqlite_reader.sqlite_db_connection() as cursor:
            for row in denormalized_data:
                insert_query = """
                    INSERT INTO denormalized_data
                    (investigation_entity_id, pdb_id, file_name, model_file_no, entity_id, type, seq_one_letter_code, chem_comp_id, src_method, description, poly_type)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                cursor.execute(
                    insert_query,
                    (
                        row["ordinal"],
                        row["pdb_id"],
                        row["file_name"],
                        row["model_file_no"],
                        row["entity_id"],
                        row["type"],
                        row["seq_one_letter_code"],
                        row["chem_comp_id"],
                        row["src_method"],
                        row["description"],
                        row["poly_type"],
                    ),
                )

    def add_descript_categories(self):
        logging.info("Adding descript categories info to the table")
        poly_descript = {}
        non_poly_descript = {}

        unique_poly = self.sql_execute(
            """
            SELECT DISTINCT(set_of_poly) FROM
                (
                    SELECT pdb_id,
                    GROUP_CONCAT(investigation_entity_id) AS set_of_poly
                    FROM 
                    (
                        SELECT pdb_id, investigation_entity_id FROM denormalized_data WHERE type="polymer" ORDER BY investigation_entity_id
                    )
                    GROUP BY pdb_id
                )
                GROUP BY set_of_poly
            """
        )

        for i, poly in enumerate(unique_poly):
            poly_descript[poly[0]] = i + 1

        all_poly_groups = self.sql_execute(
            """
            SELECT pdb_id,
            GROUP_CONCAT(investigation_entity_id) AS set_of_poly
            FROM 
            (
                SELECT pdb_id, investigation_entity_id FROM denormalized_data WHERE type="polymer" ORDER BY investigation_entity_id
            )
            GROUP BY pdb_id
            """
        )

        for group in all_poly_groups:
            pdb_id = group[0]
            poly_descript_id = poly_descript[group[1]]
            self.sql_execute(
                f"""
                UPDATE denormalized_data
                SET poly_descript = {poly_descript_id}
                WHERE pdb_id = "{pdb_id}"
                """
            )

        unique_non_poly = self.sql_execute(
            """
                SELECT DISTINCT(set_of_non_poly) FROM
                    (
                        SELECT pdb_id,
                        GROUP_CONCAT(investigation_entity_id) AS set_of_non_poly
                        FROM 
                        (
                            SELECT pdb_id, investigation_entity_id FROM denormalized_data WHERE type="non-polymer" OR type="water" ORDER BY investigation_entity_id
                        )
                        GROUP BY pdb_id
                    )
                    GROUP BY set_of_non_poly
                """
        )

        for i, non_poly in enumerate(unique_non_poly):
            non_poly_descript[non_poly[0]] = i + 1

        all_nonpoly_groups = self.sql_execute(
            """
            SELECT pdb_id,
            GROUP_CONCAT(investigation_entity_id) AS set_of_non_poly
            FROM 
            (
                SELECT pdb_id, investigation_entity_id FROM denormalized_data WHERE type="non-polymer" OR type="water" ORDER BY investigation_entity_id
            )
            GROUP BY pdb_id
            """
        )

        for group in all_nonpoly_groups:
            pdb_id = group[0]
            non_poly_descript_id = non_poly_descript[group[1]]
            self.sql_execute(
                f"""
                            UPDATE denormalized_data
                            SET nonpoly_descript = {non_poly_descript_id}
                            WHERE pdb_id = "{pdb_id}"
                            """
            )

    def add_sample_category(self):
        unique_samples = self.sql_execute(
            """
                        SELECT poly_descript, nonpoly_descript from 
                        denormalized_data GROUP BY poly_descript, nonpoly_descript"""
        )
        for index, sample in enumerate(unique_samples):
            self.sql_execute(
                f"""
                            UPDATE denormalized_data
                            SET sample_id = {index+1}
                            WHERE poly_descript = "{sample[0]}" AND 
                            nonpoly_descript = "{sample[1]}"
                            """
            )

    def add_exptl_data(self):
        exptl_data = []
        for file_name, datablock in self.reader.data.items():
            exptl_category = datablock.find_mmcif_category("_exptl")
            exptl_columns = {name: i for i, name in enumerate(exptl_category.tags)}
            database_2_category = datablock.find_mmcif_category("_database_2")
            database_2_columns = {
                name: i for i, name in enumerate(database_2_category.tags)
            }
            pdb_id = database_2_category[0][
                database_2_columns["_database_2.database_code"]
            ]

            if exptl_category is not None:
                for row in exptl_category:
                    exptl_data.append(
                        {
                            "pdb_id": pdb_id,
                            "exptl_method": row[exptl_columns["_exptl.method"]].strip("'"),
                        }
                    )
        for row in exptl_data:
            self.sql_execute(
                f"""
                            UPDATE denormalized_data
                            SET 
                                exptl_method = {repr(row['exptl_method'])}
                            WHERE 
                                pdb_id = "{row['pdb_id']}" 
                            """
            )

    def add_synchrotron_data(self):
        synchrotron_data = []
        campaigns = {}
        next_ordinal = 1
        for file_name, datablock in self.reader.data.items():
            diffrn_source_category = datablock.find_mmcif_category("_diffrn_source")
            diffrn_source_columns = {
                name: i for i, name in enumerate(diffrn_source_category.tags)
            }
            database_2_category = datablock.find_mmcif_category("_database_2")
            database_2_columns = {
                name: i for i, name in enumerate(database_2_category.tags)
            }
            pdb_id = database_2_category[0][
                database_2_columns["_database_2.database_code"]
            ]
            if diffrn_source_category is not None:
                for row in diffrn_source_category:
                    synchrotron_site = row[
                        diffrn_source_columns["_diffrn_source.pdbx_synchrotron_site"]
                    ]
                    if synchrotron_site not in campaigns:
                        campaigns[synchrotron_site] = next_ordinal
                        next_ordinal = next_ordinal + 1
                    synchrotron_data.append(
                        {
                            "pdb_id": pdb_id,
                            "synchrotron_site": synchrotron_site,
                            "campaign_id": campaigns[synchrotron_site],
                            "series_id": campaigns[synchrotron_site],
                        }
                    )
        for row in synchrotron_data:
            self.sql_execute(
                f"""
                            UPDATE denormalized_data
                            SET 
                                synchrotron_site = "{row['synchrotron_site']}",
                                campaign_id = {row['campaign_id']},
                                series_id = {row['series_id']}
                            WHERE 
                                pdb_id = "{row['pdb_id']}" 
                            """
            )

    def add_struct_ref_data(self):
        struct_ref = []
        for file_name, datablock in self.reader.data.items():
            struct_ref_category = datablock.find_mmcif_category("_struct_ref")
            database_2_category = datablock.find_mmcif_category("_database_2")
            struct_ref_columns = {
                name: i for i, name in enumerate(struct_ref_category.tags)
            }
            database_2_columns = {
                name: i for i, name in enumerate(database_2_category.tags)
            }
            pdb_id = database_2_category[0][
                database_2_columns["_database_2.database_code"]
            ]

            if struct_ref_category is not None:
                for row in struct_ref_category:
                    struct_ref.append(
                        {
                            "pdb_id": pdb_id,
                            "entity_id": row[
                                struct_ref_columns["_struct_ref.entity_id"]
                            ],
                            "db_name": row[struct_ref_columns["_struct_ref.db_name"]],
                            "db_code": row[struct_ref_columns["_struct_ref.db_code"]],
                            "pdbx_db_accession": row[
                                struct_ref_columns["_struct_ref.pdbx_db_accession"]
                            ],
                        }
                    )

        for row in struct_ref:
            self.sql_execute(
                f"""
                            UPDATE denormalized_data
                            SET 
                             db_name = "{row['db_name']}",
                             db_code = "{row['db_code']}",
                             db_accession = "{row['pdbx_db_accession']}"
                            WHERE 
                            pdb_id = "{row['pdb_id']}" AND 
                            entity_id = "{row['entity_id']}" AND
                            type = "polymer"
                            """
            )

    def add_investigation_id(self, investigation_id: str):
        self.sql_execute(
            f"""
                            UPDATE denormalized_data
                            SET investigation_id = "{investigation_id}"
                            """
        )



def download_and_run_pdbe_investigation(pdb_ids: List[str], investigation_id: str, output_path:str, json_path: str) -> None:
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

        run(temp_dir, investigation_id, output_path, json_path)

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
        run(args.model_folder, args.id,args.output_folder, args.json)
    elif args.pdb_ids:
        download_and_run_pdbe_investigation(args.pdb_ids, args.investigation_id, args.output_folder, args.json)
    elif args.csv_file:
        group_data = parse_csv(args.csv_file)
        for group, entry in group_data.items():
            try:
                download_and_run_pdbe_investigation(entry, group, args.output_folder, args.json)
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


def run(folder_path : str, investigation_id: str, output_path: str, json_path: str) -> None:
    model_file_path = get_cif_file_paths(folder_path)
    print("List of CIF file paths:")
    for file_path in model_file_path:
        print(file_path)
    im = InvestigationPdbe(model_file_path, investigation_id, output_path, json_path)
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