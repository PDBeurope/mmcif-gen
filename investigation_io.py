import logging
import sys
import gemmi
import csv
import sqlite3
from contextlib import contextmanager

logging.basicConfig(stream=sys.stdout, level=logging.INFO)


class CIFReader:
    def __init__(self) -> None:
        self.data = {}  # Dictionary to store the parsed CIF data
        self.denormalised_data = []
        self.conn = sqlite3.connect("file::memory:?cache=shared", uri=True)

    def read_files(self, file_paths):
        logging.info("Reading CIF files")
        for file_path in file_paths:
            cif_block = gemmi.cif.read_file(file_path)
            file_name = file_path.split("/")[-1]
            self.data[file_name] = cif_block.sole_block()

    # @contextmanager
    # def sqlite_db_new_connection(self):
    #     logging.debug("Creating In-memory DB connection")
    #     conn = sqlite3.connect(":memory:?cache=shared", uri=True)
    #     try:
    #         yield conn
    #     finally:
    #         conn.commit()
    #         conn.close()

    @contextmanager
    def sqlite_db_connection(self):
        logging.debug("Re-using In-memory DB connection")
        conn = self.conn
        try:
            yield conn
        finally:
            conn.commit()

    def sql_execute(self, query):
        logging.debug(f"Executing query: {query}")
        result = []
        with self.sqlite_db_connection() as conn:
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
        with self.sqlite_db_connection() as cursor:
            cursor.execute(drop_denormalized_table)
            cursor.execute(create_denormalized_table)

    def build_denormalised_data(self):
        logging.info("Building Denormalized data table from the cif files")
        denormalized_data = []
        ordinals = {}
        next_poly_ordinal = 1
        next_nonpoly_ordinal = 1
        for file_name, datablock in self.data.items():
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

        with self.sqlite_db_connection() as cursor:
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
        for file_name, datablock in self.data.items():
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
        for file_name, datablock in self.data.items():
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
        for file_name, datablock in self.data.items():
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

    def item_exists_across_all(self, category, item):
        logging.info(f"Checking existence across all model files for {category}.{item}")
        try:
            for file_name, sole_block in self.data.items():
                if sole_block.find_mmcif_category(category):
                    if not sole_block.get_mmcif_category(category)[item]:
                        logging.info("Does not exist across all files")
                        return False
        except KeyError as e:
            logging.warning(f"Missing {category}.{item} from {file_name}")
            logging.info("Does not exist across all files")
            return False
        logging.info("Exists across all files")
        return True

    def item_is_empty_in_any(self, category, item):
        logging.info(f"Checking if any items is empty in {category}.{item}")
        for file_name, sole_block in self.data.items():
            if sole_block.find_mmcif_category(category):
                try:
                    for value in sole_block.get_mmcif_category(category)[item]:
                        if value in [None, "?"]:
                            logging.info(f"Value is empty in {file_name} for {category}.{item}")
                            return True
                except Exception as e:
                    logging.warning(f"Missing field {category}.{item} from {file_name}")
                    return True
        logging.info("Value is non empty in all files")
        return False

    def collate_item_per_file(self, category, item):
        logging.info(f"Collating with distinct files {category}.{item}")
        collated_data = {}
        for file_no, (file_name, sole_block) in enumerate(self.data.items()):
            collated_data[file_no] = []
            if sole_block.find_mmcif_category(category):
                for values in sole_block.get_mmcif_category(category)[item]:
                    collated_data[file_no].append(values)
        return collated_data

    def collate_item(self, category, item):
        logging.info(f"Collating {category}.{item}")
        collated_data = []
        try:
            for file_name, sole_block in self.data.items():
                if sole_block.find_mmcif_category(category):
                    for values in sole_block.get_mmcif_category(category)[item]:
                        collated_data.append(values)
        except KeyError as e:
            logging.exception(f"Missing {category}.{item} from {file_name}")
            raise Exception(e)
        return collated_data

    def collate_items(self, category, items):
        logging.info(f"Collating multiple items in {category} items: {items}")
        collated_data = {}
        for file_name, sole_block in self.data.items():
            try:
                if sole_block.find_mmcif_category(category):
                    for item in items:
                        collated_data.setdefault(item, [])
                        for values in sole_block.get_mmcif_category(category)[item]:
                            collated_data[item].append(values)
            except Exception as e:
                logging.exception(f"Missing {category}.{item} from {file_name}")
                raise Exception(e)
        return collated_data

    def collate_category(self, category):
        logging.info(f"Collating all items in {category}")
        collated_data = {}
        for file_name, sole_block in self.data.items():
            if sole_block.find_mmcif_category(category):
                for item, values in sole_block.get_mmcif_category(category).items():
                    collated_data.setdefault(item, [])
                    collated_data[item].extend(values)
        return collated_data

    def get_data(self, category, items):
        filtered_data = []
        for file_name, sole_block in self.data.items():
            filtered_data.append(sole_block.get_mmcif_category(category)[items])
        return filtered_data

    def get_rows_in_category(self, category):
        for file_name, sole_block in self.data.items():
            if sole_block.find_mmcif_category(category):
                items = sole_block.get_mmcif_category(category)
                first_item = items.keys()[0]
                return len(first_item)


class InvestigationStorage:
    def __init__(self):
        self.data = {}
        self.mmcif_order = {}

    def add_category(self, category_name):
        if category_name not in self.data:
            self.data[category_name] = {}

    def set_item(self, category_name, item_name, item_value):
        if category_name not in self.data:
            self.add_category(category_name)
        self.data[category_name][item_name] = item_value

    def get_category_data(self, category):
        return self.data[category]

    def get_item_data(self, category, item):
        return self.data[category][item]

    def get_items_data(self, category, items):
        result = {}
        for item in items:
            result[item] = self.data[category][item]
        return result

    def set_items(self, category_name, data):
        if category_name not in self.data:
            self.add_category(category_name)
        for item, values in data.items():
            if item not in self.data[category_name]:
                self.data[category_name][item] = []
            for value in values:
                self.data[category_name][item].append(value)

    def get_data(self) -> dict:
        return self.data
    
    def get_item_order(self, category) -> list:
        return self.mmcif_order.get(category, [])

    def write_data_to_cif(self, output_file) -> None:
        logging.info("Writing Investigation cif file")
        write_options = gemmi.cif.WriteOptions()
        write_options.align_loops = 50
        write_options.align_pairs = 50

        doc = gemmi.cif.Document()
        block = doc.add_new_block("PDBX_Investigation")
        for category, items in self.data.items():
            ordered_category = {}
            ordered_items = self.get_item_order(category)
            for ordered_item in ordered_items:
                ordered_category[ordered_item]  = items.pop(ordered_item)
            ordered_category.update(items)
            block.set_mmcif_category(category, ordered_category)
        block.write_file(output_file, write_options)

    def integrity_check(self):
        inconsistent_keys = {}
        for dictionary_key, dictionary_values in self.data.items():
            max_length = max(len(values) for values in dictionary_values.values())
            inconsistent_lists = [
                (key, len(values))
                for key, values in dictionary_values.items()
                if len(values) != max_length
            ]
            if inconsistent_lists:
                inconsistent_keys[dictionary_key] = inconsistent_lists

        for dictionary_key, keys_lengths in inconsistent_keys.items():
            print(f"Dictionary '{dictionary_key}' has inconsistent list lengths:")
            for key, length in keys_lengths:
                print(f"   Key '{key}' has length {length}")


class ExternalInformation:
    def __init__(self, filename) -> None:
        self.filename = filename
        self.inchi_keys = {}

    def _load_inchi_keys(self):
        if self.inchi_keys:
            return
        logging.info("Loading Inchikeys csv file")
        with open(self.filename, mode="r", newline="") as file:
            reader = csv.DictReader(file)
            for row in reader:
                chem_comp_id = row["CHEM_COMP_ID"]
                descriptor = row["DESCRIPTOR"]
                self.inchi_keys[chem_comp_id] = descriptor

    def _get_inchi_key(self, chem_comp_id):
        return self.inchi_keys.get(chem_comp_id)

    def get_inchi_key(self, chem_comp_id):
        self._load_inchi_keys()
        return self._get_inchi_key(chem_comp_id)
