import logging
import sys
import gemmi
import csv
import sqlite3
from contextlib import contextmanager
import requests
import jq
import pickle
import os
import json
logging.basicConfig(stream=sys.stdout, level=logging.INFO)


class CIFReader:
    def __init__(self) -> None:
        self.data = {}  # Dictionary to store the parsed CIF data
        

    def read_files(self, file_paths):
        logging.info("Reading CIF files")
        for file_path in file_paths:
            cif_block = gemmi.cif.read_file(file_path)
            file_name = file_path.split("/")[-1]
            self.data[file_name] = cif_block.sole_block()
    
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

class SqliteReader:
    def __init__(self, sqlite_path) -> None:
        self.data = {} 
        self.denormalised_data = []
        self.conn = sqlite3.connect(sqlite_path, uri=True)

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
    
    def create_table_from_csv(self, csv_file):
        logging.info(f"Creating table from CSV file: {csv_file}")
        table_name = os.path.splitext(os.path.basename(csv_file))[0]

        # DROP TABLE IF EXISTS
        drop_table_query = f"DROP TABLE IF EXISTS \"{table_name}\""
        self.sql_execute(drop_table_query)
        
        with open(csv_file, 'r', newline='') as f:
            csv_reader = csv.reader(f)
            headers = next(csv_reader)
            
            # Read a sample of rows to determine column types
            sample_rows = []
            for _ in range(100):  # Read up to 100 rows as a sample
                try:
                    sample_rows.append(next(csv_reader))
                except StopIteration:
                    break
            
            column_types = self._determine_column_types(headers, sample_rows)
            
            # Create the table
            create_table_query = f"CREATE TABLE IF NOT EXISTS \"{table_name}\" ({', '.join([f'`{header}` {column_types[header]}' for header in headers])})"
            self.sql_execute(create_table_query)
            
            # Insert data
            insert_query = f"INSERT INTO \"{table_name}\" ({', '.join([f'`{header}`' for header in headers])}) VALUES ({', '.join(['?' for _ in headers])})"
            with self.sqlite_db_connection() as conn:
                cursor = conn.cursor()
                cursor.executemany(insert_query, sample_rows)
                
                # Continue inserting the rest of the data
                f.seek(0)
                next(csv_reader)  # Skip header
                for row in csv_reader:
                    if row not in sample_rows:
                        cursor.execute(insert_query, row)
                
            logging.info(f"Table '{table_name}' created and populated with data from {csv_file}")

    def _determine_column_types(self, headers, sample_rows):
        column_types = {}
        for header in headers:
            column_data = [row[headers.index(header)] for row in sample_rows if row[headers.index(header)]]
            if all(self._is_integer(value) for value in column_data):
                column_types[header] = 'INTEGER'
            elif all(self._is_float(value) for value in column_data):
                column_types[header] = 'REAL'
            else:
                column_types[header] = 'TEXT'
        return column_types

    @staticmethod
    def _is_integer(value):
        try:
            int(value)
            return True
        except ValueError:
            return False

    @staticmethod
    def _is_float(value):
        try:
            float(value)
            return True
        except ValueError:
            return False

class InvestigationStorage:
    def __init__(self, investigation_id):
        self.data = {}
        self.mmcif_order = {}
        self.investigation_id = investigation_id

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
        block = doc.add_new_block(f"{self.investigation_id}")
        for category, items in self.data.items():
            ordered_category = {}
            ordered_items = self.get_item_order(category)
            for ordered_item in ordered_items:
                if ordered_item in items:
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

class PickleReader:
    def __init__(self, pickle_path):
        self.data = {}
        self.pickle_path = pickle_path
        self.load_pickle()

    def load_pickle(self):
        if self.pickle_path:
            with open(self.pickle_path, 'rb') as file:
                self.data = pickle.load(file)

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
    
class JsonReader:
    def __init__(self, json_path):
        self.data = {}
        self.json_path = json_path
        self.load_json()

    def load_json(self):
        with open(self.json_path, 'r') as file:
            self.data = json.load(file)
    
    def jq_filter(self, filter_string: str):
        """
        Apply a JQ filter to the JSON data
        
        Args:
            filter_string (str): The JQ filter to apply
            
        Returns:
            The filtered data - could be a single value, list, or dict depending on the filter
        """
        try:
            return jq.compile(filter_string).input(self.data).first()
        except Exception as e:
            logging.error(f"Failed to apply JQ filter: {filter_string}")
            logging.exception(e)
            raise

class RestReader:
    def __init__(self, base_url, username, password):
        self.base_url = base_url
        self.session = requests.Session()
        self.access_token = self.get_auth_token(username, password)
        self.session.headers.update({'Authorization': f'Bearer {self.access_token}'})

    def get_auth_token(self, username, password):
        url = f"{self.base_url}/auth"
        payload = {"grant_type": "password", 'username': username, 'password': password}
        response = self.session.post(url, json=payload)
        response.raise_for_status()
        return response.json()['access_token']

    def get(self, endpoint, params=None, filter_query=None):
        url = f"{self.base_url}{endpoint}"
        response = self.session.get(url, params=params)
        response.raise_for_status()
        return self.filter_response(response.json(), filter_query)

    def post(self, endpoint, data=None, filter_query=None):
        url = f"{self.base_url}{endpoint}"
        response = self.session.post(url, json=data)
        response.raise_for_status()
        return self.filter_response(response.json(), filter_query)

    def filter_response(self, response_json, filter_query):
        if filter_query:
            return jq.compile(filter_query).input(response_json).first()
        return response_json