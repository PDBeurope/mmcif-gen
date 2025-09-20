"""
PDBe Investigation Facility for mmCIF Investigation File Generation

This module provides functionality to process PDB mmCIF files and generate
investigation files for ligand screening workflows. It supports:

- Batch processing of mmCIF files with memory management
- Taxonomy data extraction from multiple sources (natural, engineered, synthetic)
- Robust download functionality with fallback URLs
- Comprehensive error handling and validation
- Configurable processing parameters
- Progress tracking and performance monitoring

Key Components:
- InvestigationPdbe: Main processing class
- PdbeConfig: Configuration management
- Validation functions for PDB IDs and file formats
- Download and cleanup utilities

Usage:
    # Process local files
    processor = InvestigationPdbe(file_paths, "investigation_id", "output_path")
    processor.pre_run()
    processor.run()
    
    # Download and process PDB IDs
    download_and_run_pdbe_investigation(["1abc", "2def"], "inv_id", "output", "config.json")
"""

from mmcif_gen.investigation_engine import InvestigationEngine
from mmcif_gen.investigation_io import CIFReader, SqliteReader
import os
import requests
from typing import List, Dict, Optional
import sys
import logging
import gzip
import tempfile
import shutil
import csv
from contextlib import contextmanager
import sqlite3
import time
from dataclasses import dataclass
import json
import re

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

FTP_URL_UPDATED = (
    "https://ftp.ebi.ac.uk/pub/databases/msd/updated_mmcif/divided/{}/{}_updated.cif.gz"
)
FTP_URL_ARCHIVE = (
    "https://ftp.ebi.ac.uk/pub/databases/pdb/data/structures/divided/mmCIF/{}/{}.cif.gz"
)

@dataclass
class PdbeConfig:
    """Configuration for PDBe investigation processing."""
    batch_size: int = 1000
    download_timeout: int = 30
    max_retries: int = 3
    validate_mmcif_format: bool = True
    cleanup_temp_files: bool = True
    log_level: str = "INFO"
    
    @classmethod
    def from_file(cls, config_path: str) -> 'PdbeConfig':
        """Load configuration from JSON file."""
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                config_data = json.load(f)
            return cls(**config_data)
        return cls()


def validate_pdb_id(pdb_id: str) -> bool:
    """
    Validate PDB ID format (4 characters: digit + 3 alphanumeric).
    
    Args:
        pdb_id: PDB ID to validate
        
    Returns:
        True if valid PDB ID format, False otherwise
    """
    if not pdb_id or len(pdb_id) != 4:
        return False
    
    # PDB ID format: first character is digit, followed by 3 alphanumeric
    pattern = r'^[0-9][A-Za-z0-9]{3}$'
    return bool(re.match(pattern, pdb_id))


def validate_pdb_ids(pdb_ids: List[str]) -> List[str]:
    """
    Validate a list of PDB IDs and return only valid ones.
    
    Args:
        pdb_ids: List of PDB IDs to validate
        
    Returns:
        List of valid PDB IDs
        
    Raises:
        ValueError: If no valid PDB IDs found
    """
    valid_ids = []
    invalid_ids = []
    
    for pdb_id in pdb_ids:
        if validate_pdb_id(pdb_id.upper()):
            valid_ids.append(pdb_id.upper())
        else:
            invalid_ids.append(pdb_id)
    
    if invalid_ids:
        logging.warning(f"Invalid PDB IDs found: {invalid_ids}")
    
    if not valid_ids:
        raise ValueError(f"No valid PDB IDs found in: {pdb_ids}")
    
    logging.info(f"Validated {len(valid_ids)} PDB IDs: {valid_ids}")
    return valid_ids


class InvestigationPdbe(InvestigationEngine):
    """
    PDBe Investigation processor for generating mmCIF investigation files.
    
    Processes PDB mmCIF files to extract entity information, taxonomy data,
    and experimental metadata for ligand screening investigations.
    
    Attributes:
        reader: CIFReader instance for parsing mmCIF files
        sqlite_reader: SqliteReader for in-memory database operations
        model_file_path: List of mmCIF file paths to process
    """
    
    # Database configuration
    BATCH_SIZE = 1000  # Default batch size (overridden by config.batch_size)
    DEFAULT_DB_NAME = "pdbe_sqlite.db"
    
    # mmCIF field mappings for entity processing
    REQUIRED_ENTITY_FIELDS = ["_entity.id", "_entity.type", "_entity.src_method"]
    
    # Taxonomy source category mappings
    # nat = natural source, man = genetically engineered, syn = synthetic
    TAXONOMY_CATEGORIES = {
        "nat": "_entity_src_nat",
        "man": "_entity_src_gen", 
        "syn": "_entity_src_syn"
    }
        
    def __init__(self, model_file_path: List[str], investigation_id: str, output_path: str, 
                 pdbe_investigation_json: str="./operations/pdbe/pdbe_investigation.json",
                 config: Optional[PdbeConfig] = None) -> None:
        """
        Initialize PDBe Investigation processor.
        
        Args:
            model_file_path: List of paths to mmCIF files
            investigation_id: Unique identifier for the investigation
            output_path: Directory for output files
            pdbe_investigation_json: Path to operation configuration file
            config: Optional configuration object, uses defaults if None
        """
        logging.info("Instantiating PDBe Investigation subclass")
        self.reader = CIFReader()
        self.model_file_path = model_file_path
        self.operation_file_json = pdbe_investigation_json
        
        # Initialize configuration
        self.config = config or PdbeConfig()
        
        # Use configured database name and batch size
        self.sqlite_reader = SqliteReader(self.DEFAULT_DB_NAME)
        super().__init__(investigation_id, output_path)

    def validate_inputs(self) -> bool:
        """Validate input files and parameters before processing."""
        if not self.model_file_path:
            logging.error("No model files provided")
            return False
        
        for file_path in self.model_file_path:
            if not os.path.exists(file_path):
                logging.error(f"Model file not found: {file_path}")
                return False
            if not file_path.endswith('.cif'):
                logging.warning(f"File may not be mmCIF format: {file_path}")
        
        if not os.path.exists(self.operation_file_json):
            logging.error(f"Operation file not found: {self.operation_file_json}")
            return False
        
        return True

    def safe_extract_field(self, row: List, columns: Dict[str, int], 
                          field_name: str, default: str = "") -> str:
        """Safely extract a field from mmCIF row with error handling."""
        try:
            if field_name in columns:
                value = row[columns[field_name]]
                return self.clean_text_field(value) if value else default
            else:
                logging.debug(f"Field {field_name} not found in category")
                return default
        except (IndexError, KeyError) as e:
            logging.warning(f"Error extracting field {field_name}: {e}")
            return default

    def pre_run(self) -> None:
        logging.info("Pre-running")
        
        # Validate inputs first
        if not self.validate_inputs():
            raise ValueError("Input validation failed")
        
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
                seq_one_letter_code_can TEXT,
                organism_scientific TEXT,
                ncbi_taxonomy_id TEXT,
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

    def clean_text_field(self, text):
        """Clean text field by removing common formatting artifacts"""
        if not text:
            return ""
        return text.strip("'").strip('"').strip(";").strip("\n").strip()

    def build_taxonomy_lookups(self, nat_taxonomy_category, man_taxonomy_category, 
                              syn_taxonomy_category, nat_tax_columns, man_tax_columns, 
                              syn_tax_columns) -> Dict[str, Dict[str, Dict[str, str]]]:
        """
        Build taxonomy lookup dictionaries from mmCIF categories.
        
        Returns:
            Dict with keys 'nat', 'man', 'syn' containing entity_id -> taxonomy mappings
        """
        taxonomy_lookups = {"nat": {}, "man": {}, "syn": {}}
        
        # Natural source taxonomy
        if nat_taxonomy_category is not None:
            for row in nat_taxonomy_category:
                try:
                    entity_id = row[nat_tax_columns["_entity_src_nat.entity_id"]]
                    taxonomy_lookups["nat"][entity_id] = {
                        "organism_scientific": self.clean_text_field(
                            row[nat_tax_columns["_entity_src_nat.pdbx_organism_scientific"]]
                        ),
                        "ncbi_taxonomy_id": self.clean_text_field(
                            row[nat_tax_columns["_entity_src_nat.pdbx_ncbi_taxonomy_id"]]
                        )
                    }
                except (KeyError, IndexError) as e:
                    logging.warning(f"Error processing natural taxonomy for entity {entity_id}: {e}")
        
        # Genetically engineered source taxonomy
        if man_taxonomy_category is not None:
            for row in man_taxonomy_category:
                try:
                    entity_id = row[man_tax_columns["_entity_src_gen.entity_id"]]
                    taxonomy_lookups["man"][entity_id] = {
                        "organism_scientific": self.clean_text_field(
                            row[man_tax_columns["_entity_src_gen.pdbx_gene_src_scientific_name"]]
                        ),
                        "ncbi_taxonomy_id": self.clean_text_field(
                            row[man_tax_columns["_entity_src_gen.pdbx_ncbi_taxonomy_id"]]
                        )
                    }
                except (KeyError, IndexError) as e:
                    logging.warning(f"Error processing genetically engineered taxonomy for entity {entity_id}: {e}")
        
        # Synthetic source taxonomy
        if syn_taxonomy_category is not None:
            for row in syn_taxonomy_category:
                try:
                    entity_id = row[syn_tax_columns["_pdbx_entity_src_syn.entity_id"]]
                    taxonomy_lookups["syn"][entity_id] = {
                        "organism_scientific": self.clean_text_field(
                            row[syn_tax_columns["_pdbx_entity_src_syn.organism_scientific"]]
                        ),
                        "ncbi_taxonomy_id": self.clean_text_field(
                            row[syn_tax_columns["_pdbx_entity_src_syn.ncbi_taxonomy_id"]]
                        )
                    }
                except (KeyError, IndexError) as e:
                    logging.warning(f"Error processing synthetic taxonomy for entity {entity_id}: {e}")
        
        return taxonomy_lookups

    def build_polymer_lookup(self, entity_poly_category, poly_columns) -> Dict[str, Dict[str, str]]:
        """Build a lookup dictionary for polymer data to avoid O(n²) complexity."""
        polymer_lookup = {}
        
        if entity_poly_category is not None:
            for poly_row in entity_poly_category:
                try:
                    entity_id = self.safe_extract_field(poly_row, poly_columns, "_entity_poly.entity_id")
                    if entity_id:
                        polymer_lookup[entity_id] = {
                            "seq_one_letter_code": self.safe_extract_field(
                                poly_row, poly_columns, "_entity_poly.pdbx_seq_one_letter_code"
                            ),
                            "seq_one_letter_code_can": self.safe_extract_field(
                                poly_row, poly_columns, "_entity_poly.pdbx_seq_one_letter_code_can"
                            ),
                            "poly_type": self.safe_extract_field(
                                poly_row, poly_columns, "_entity_poly.type"
                            )
                        }
                except Exception as e:
                    logging.warning(f"Error processing polymer data: {e}")
        
        return polymer_lookup

    def build_nonpoly_lookup(self, entity_nonpoly_category, nonpoly_columns) -> Dict[str, str]:
        """Build a lookup dictionary for non-polymer data."""
        nonpoly_lookup = {}
        
        if entity_nonpoly_category is not None:
            for nonpoly_row in entity_nonpoly_category:
                try:
                    entity_id = self.safe_extract_field(nonpoly_row, nonpoly_columns, "_pdbx_entity_nonpoly.entity_id")
                    if entity_id:
                        nonpoly_lookup[entity_id] = self.safe_extract_field(
                            nonpoly_row, nonpoly_columns, "_pdbx_entity_nonpoly.comp_id"
                        )
                except Exception as e:
                    logging.warning(f"Error processing non-polymer data: {e}")
        
        return nonpoly_lookup

    def process_entities_in_batches(self, denormalized_data: List[Dict]) -> None:
        """Process entities in batches to manage memory usage."""
        batch_data = []
        
        for entity_data in denormalized_data:
            batch_data.append(entity_data)
            
            if len(batch_data) >= self.config.batch_size:
                self._insert_batch(batch_data)
                batch_data = []
                logging.debug(f"Processed batch of {self.config.batch_size} entities")
        
        # Process remaining entities
        if batch_data:
            self._insert_batch(batch_data)
            logging.debug(f"Processed final batch of {len(batch_data)} entities")

    def _insert_batch(self, batch_data: List[Dict]) -> None:
        """Insert a batch of entities into the database with error handling."""
        try:
            with self.sqlite_reader.sqlite_db_connection() as cursor:
                insert_query = """
                    INSERT INTO denormalized_data
                    (investigation_entity_id, pdb_id, file_name, model_file_no, entity_id, type, 
                     seq_one_letter_code, seq_one_letter_code_can, chem_comp_id, src_method, 
                     description, poly_type, organism_scientific, ncbi_taxonomy_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                cursor.executemany(insert_query, [
                    (
                        row["ordinal"], row["pdb_id"], row["file_name"], row["model_file_no"],
                        row["entity_id"], row["type"], row["seq_one_letter_code"], 
                        row["seq_one_letter_code_can"], row["chem_comp_id"], row["src_method"],
                        row["description"], row["poly_type"], row["organism_scientific"], 
                        row["ncbi_taxonomy_id"]
                    ) for row in batch_data
                ])
                logging.debug(f"Successfully inserted batch of {len(batch_data)} entities")
        except sqlite3.Error as e:
            logging.error(f"Database error inserting batch: {e}")
            # Log problematic data for debugging
            logging.debug(f"Batch data sample: {batch_data[0] if batch_data else 'Empty batch'}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error inserting batch: {e}")
            raise

    def build_denormalised_data(self):
        """Build denormalized data with streaming batch processing to manage memory."""
        start_time = time.time()
        logging.info("Building Denormalized data table from the cif files")
        
        # Use streaming approach instead of loading all data into memory
        batch_data = []
        ordinals = {}
        next_poly_ordinal = 1
        next_nonpoly_ordinal = 1
        
        total_files = len(self.reader.data)
        processed_entities = 0
        
        for file_idx, (file_name, datablock) in enumerate(self.reader.data.items()):
            logging.info(f"Processing file {file_idx + 1}/{total_files}: {file_name}")
            entities_in_file = 0
            entity_category = datablock.find_mmcif_category("_entity")
            entity_poly_category = datablock.find_mmcif_category("_entity_poly")
            nat_taxonomy_category = datablock.find_mmcif_category("_entity_src_nat")
            man_taxonomy_category = datablock.find_mmcif_category("_entity_src_gen")
            syn_taxonomy_category = datablock.find_mmcif_category("_entity_src_syn")
            entity_nonpoly_category = datablock.find_mmcif_category("_pdbx_entity_nonpoly")
            database_2_category = datablock.find_mmcif_category("_database_2")

            # Create dictionaries to map column names to their indices
            entity_columns = {name: i for i, name in enumerate(entity_category.tags)}
            poly_columns = {name: i for i, name in enumerate(entity_poly_category.tags)}

            nat_tax_columns = {}
            man_tax_columns = {}
            syn_tax_columns = {}

            if nat_taxonomy_category is not None:
                nat_tax_columns = {name: i for i, name in enumerate(nat_taxonomy_category.tags)}
            if man_taxonomy_category is not None:
                man_tax_columns = {name: i for i, name in enumerate(man_taxonomy_category.tags)}
            if syn_taxonomy_category is not None:
                syn_tax_columns = {name: i for i, name in enumerate(syn_taxonomy_category.tags)}

            nonpoly_columns = {}
            if entity_nonpoly_category is not None:
                nonpoly_columns = {name: i for i, name in enumerate(entity_nonpoly_category.tags)}
            
            database_2_columns = {}
            if database_2_category is not None:
                database_2_columns = {name: i for i, name in enumerate(database_2_category.tags)}

            pdb_id = ""
            if database_2_category is not None and len(database_2_category) > 0:
                try:
                    pdb_id = database_2_category[0][database_2_columns["_database_2.database_code"]]
                except (KeyError, IndexError) as e:
                    logging.warning(f"Could not extract PDB ID from {file_name}: {e}")
                    pdb_id = file_name.replace('.cif', '').upper()  # Fallback to filename

            # Create taxonomy lookup dictionaries using helper method
            taxonomy_lookups = self.build_taxonomy_lookups(
                nat_taxonomy_category, man_taxonomy_category, syn_taxonomy_category,
                nat_tax_columns, man_tax_columns, syn_tax_columns
            )
            
            # Build optimized lookups to avoid O(n²) complexity
            polymer_lookup = self.build_polymer_lookup(entity_poly_category, poly_columns)
            nonpoly_lookup = self.build_nonpoly_lookup(entity_nonpoly_category, nonpoly_columns)

            if entity_category is not None:
                for row in entity_category:
                    try:
                        entity_id = self.safe_extract_field(row, entity_columns, "_entity.id")
                        entity_type = self.safe_extract_field(row, entity_columns, "_entity.type")
                        src_method = self.safe_extract_field(row, entity_columns, "_entity.src_method")
                        description = self.safe_extract_field(row, entity_columns, "_entity.pdbx_description")
                    except Exception as e:
                        logging.warning(f"Error extracting entity data from {file_name}: {e}")
                        continue
                    
                    # Initialize all variables
                    chem_comp_id = ""
                    seq_one_letter_code = ""
                    seq_one_letter_code_can = ""
                    poly_type = ""
                    ordinal = ""
                    organism_scientific = ""
                    ncbi_taxonomy_id = ""
                    
                    # Validate required fields
                    if not pdb_id:
                        logging.warning(f"Missing PDB ID for file {file_name}, skipping entity {entity_id}")
                        continue
                    if not entity_id:
                        logging.warning(f"Missing entity ID in file {file_name}, skipping")
                        continue
            
                    # Determine taxonomy source based on src_method and lookup taxonomy data
                    if src_method in taxonomy_lookups and entity_id in taxonomy_lookups[src_method]:
                        try:
                            organism_scientific = taxonomy_lookups[src_method][entity_id]["organism_scientific"]
                            ncbi_taxonomy_id = taxonomy_lookups[src_method][entity_id]["ncbi_taxonomy_id"]
                        except KeyError as e:
                            logging.warning(f"Missing taxonomy field for entity {entity_id}: {e}")
                            organism_scientific = ""
                            ncbi_taxonomy_id = ""
                    src_method = self.clean_text_field(src_method)    

                    if entity_type == "polymer":
                        # Use optimized polymer lookup (O(1) instead of O(n))
                        if entity_id in polymer_lookup:
                            polymer_data = polymer_lookup[entity_id]
                            seq_one_letter_code = polymer_data.get("seq_one_letter_code", "")
                            seq_one_letter_code_can = polymer_data.get("seq_one_letter_code_can", "")
                            poly_type = polymer_data.get("poly_type", "")

                        ordinal = ordinals.get(seq_one_letter_code, False)
                        if not ordinal:
                            ordinal = next_poly_ordinal
                            ordinals[seq_one_letter_code] = next_poly_ordinal
                            next_poly_ordinal = next_poly_ordinal + 1

                    elif entity_type in ["water", "non-polymer"]:
                        # Use optimized non-polymer lookup (O(1) instead of O(n))
                        if entity_id in nonpoly_lookup:
                            chem_comp_id = nonpoly_lookup[entity_id]
                            
                        ordinal = ordinals.get(chem_comp_id, False)
                        if not ordinal:
                            ordinal = next_nonpoly_ordinal
                            ordinals[chem_comp_id] = ordinal
                            next_nonpoly_ordinal = next_nonpoly_ordinal + 1

                    # Add to batch for streaming processing
                    entity_data = {
                        "ordinal": ordinal,
                        "pdb_id": pdb_id,
                        "file_name": file_name,
                        "model_file_no": "",  
                        "entity_id": entity_id,
                        "type": entity_type,
                        "seq_one_letter_code": seq_one_letter_code.strip(";").rstrip('\n'),
                        "seq_one_letter_code_can": seq_one_letter_code_can.strip(";").rstrip('\n'),
                        "chem_comp_id": chem_comp_id,
                        "src_method": src_method,
                        "poly_type": poly_type.strip("'"),
                        "description": description,
                        "organism_scientific": organism_scientific,
                        "ncbi_taxonomy_id": ncbi_taxonomy_id,
                    }
                    
                    batch_data.append(entity_data)
                    entities_in_file += 1
                    
                    # Process batch when it reaches configured size
                    if len(batch_data) >= self.config.batch_size:
                        self._insert_batch(batch_data)
                        batch_data = []
                        logging.debug(f"Processed batch of {self.config.batch_size} entities")
            
            processed_entities += entities_in_file
            logging.debug(f"Processed {entities_in_file} entities from {file_name}")
        
        # Process any remaining entities in the final batch
        if batch_data:
            self._insert_batch(batch_data)
            logging.debug(f"Processed final batch of {len(batch_data)} entities")
        
        logging.info("Successfully built and loaded data into In-memory Sqlite")
        
        # Performance summary
        end_time = time.time()
        processing_time = end_time - start_time
        entities_per_second = processed_entities / processing_time if processing_time > 0 else 0
        
        logging.info(f"Processing completed:")
        logging.info(f"  - Files processed: {total_files}")
        logging.info(f"  - Entities processed: {processed_entities}")
        logging.info(f"  - Processing time: {processing_time:.2f} seconds")
        logging.info(f"  - Throughput: {entities_per_second:.1f} entities/second")

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


def cleanup_temp_files(temp_dir: str, pdb_ids: List[str]) -> None:
    """Clean up temporary files with proper error handling."""
    try:
        for pdb_code in pdb_ids:
            for ext in [".cif.gz", ".cif"]:
                file_path = os.path.join(temp_dir, f"{pdb_code}{ext}")
                if os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                    except OSError as e:
                        logging.warning(f"Failed to remove {file_path}: {e}")
        
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
            logging.debug(f"Cleaned up temporary directory: {temp_dir}")
    except Exception as e:
        logging.error(f"Error during cleanup: {e}")


def download_and_run_pdbe_investigation(pdb_ids: List[str], investigation_id: str, 
                                       output_path: str, json_path: str) -> None:
    """
    Download PDB files and run investigation with improved error handling and progress tracking.
    
    Args:
        pdb_ids: List of PDB IDs to download
        investigation_id: Unique identifier for the investigation
        output_path: Directory for output files
        json_path: Path to operation configuration file
    """
    logging.info(f"Creating investigation files for {len(pdb_ids)} PDB IDs: {pdb_ids}")
    
    # Validate PDB IDs first
    try:
        validated_pdb_ids = validate_pdb_ids(pdb_ids)
    except ValueError as e:
        logging.error(f"PDB ID validation failed: {e}")
        raise
    
    temp_dir = tempfile.mkdtemp()
    downloaded_files = []
    failed_downloads = []
    
    try:
        for idx, pdb_code in enumerate(validated_pdb_ids):
            logging.info(f"Downloading {idx + 1}/{len(validated_pdb_ids)}: {pdb_code}")
            
            # Try updated version first, then archive
            urls = [
                FTP_URL_UPDATED.format(pdb_code[1:3], pdb_code),
                FTP_URL_ARCHIVE.format(pdb_code[1:3], pdb_code)
            ]
            
            success = False
            for url_type, url in enumerate(["updated", "archive"]):
                try:
                    response = requests.get(urls[url_type], timeout=30)
                    if response.status_code == 200:
                        compressed_file_path = os.path.join(temp_dir, f"{pdb_code}.cif.gz")
                        uncompressed_file_path = os.path.join(temp_dir, f"{pdb_code}.cif")
                        
                        with open(compressed_file_path, "wb") as f:
                            f.write(response.content)

                        with gzip.open(compressed_file_path, "rb") as gz:
                            with open(uncompressed_file_path, "wb") as f:
                                f.write(gz.read())
                        
                        logging.info(f"Downloaded and unzipped {pdb_code}.cif from {url_type}")
                        success = True
                        downloaded_files.append(pdb_code)
                        break
                except requests.RequestException as e:
                    logging.warning(f"Failed to download {pdb_code} from {url_type}: {e}")
            
            if not success:
                failed_downloads.append(pdb_code)
                logging.error(f"Failed to download {pdb_code} from all sources")
        
        if downloaded_files:
            logging.info(f"Successfully downloaded {len(downloaded_files)} files")
            run(temp_dir, investigation_id, output_path, json_path)
        else:
            raise ValueError("No files were successfully downloaded")
            
        if failed_downloads:
            logging.warning(f"Failed to download {len(failed_downloads)} files: {failed_downloads}")
    
    except Exception as e:
        logging.exception(f"An error occurred during download and processing: {str(e)}")
        raise
    finally:
        # Cleanup with better error handling
        cleanup_temp_files(temp_dir, validated_pdb_ids)

def run_investigation_pdbe(args) -> None:
    """
    Main entry point for PDBe investigation processing.
    
    Args:
        args: Parsed command line arguments containing processing options
    """
    try:
        if args.model_folder:
            run(args.model_folder, args.id, args.output_folder, args.json)
        elif args.pdb_ids:
            download_and_run_pdbe_investigation(args.pdb_ids, args.investigation_id, 
                                               args.output_folder, args.json)
        elif args.csv_file:
            group_data = parse_csv(args.csv_file)
            successful_groups = 0
            failed_groups = 0
            
            for group, entry in group_data.items():
                try:
                    logging.info(f"Processing group '{group}' with {len(entry)} entries")
                    download_and_run_pdbe_investigation(entry, group, args.output_folder, args.json)
                    successful_groups += 1
                except Exception as e:
                    logging.exception(f"Failed to process group '{group}': {e}")
                    failed_groups += 1
            
            logging.info(f"Group processing completed: {successful_groups} successful, {failed_groups} failed")
        else:
            raise ValueError("PDBe Facility requires parameter: --model-folder OR --csv-file OR --pdb-ids")
            
    except Exception as e:
        logging.error(f"Investigation processing failed: {e}")
        raise


def get_cif_file_paths(folder_path: str) -> List[str]:
    """
    Get CIF file paths with improved validation and filtering.
    
    Args:
        folder_path: Directory to search for CIF files
        
    Returns:
        List of valid CIF file paths
        
    Raises:
        ValueError: If no valid CIF files found
    """
    if not os.path.exists(folder_path):
        raise ValueError(f"Folder path does not exist: {folder_path}")
    
    if not os.path.isdir(folder_path):
        raise ValueError(f"Path is not a directory: {folder_path}")
    
    cif_file_paths = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            # More precise CIF file detection
            if file.lower().endswith('.cif') and not file.lower().endswith('.cif.gz'):
                file_path = os.path.join(root, file)
                # Validate file is readable
                try:
                    with open(file_path, 'r') as f:
                        # Quick check for mmCIF format
                        first_line = f.readline().strip()
                        if first_line.startswith('data_') or 'mmcif' in first_line.lower():
                            cif_file_paths.append(file_path)
                        else:
                            logging.warning(f"File may not be valid mmCIF format: {file}")
                except (IOError, UnicodeDecodeError) as e:
                    logging.warning(f"Cannot read file {file}: {e}")
    
    if not cif_file_paths:
        raise ValueError(f"No valid CIF files found in folder: {folder_path}")
    
    logging.info(f"Found {len(cif_file_paths)} valid CIF files")
    return cif_file_paths


def run(folder_path: str, investigation_id: str, output_path: str, json_path: str) -> None:
    """
    Run investigation processing with enhanced error handling and validation.
    
    Args:
        folder_path: Directory containing CIF files
        investigation_id: Unique identifier for the investigation
        output_path: Directory for output files
        json_path: Path to operation configuration file
    """
    try:
        # Validate output directory
        if not os.path.exists(output_path):
            os.makedirs(output_path, exist_ok=True)
            logging.info(f"Created output directory: {output_path}")
        
        model_file_path = get_cif_file_paths(folder_path)
        logging.info(f"Found {len(model_file_path)} CIF files to process:")
        
        # Log file paths at debug level to avoid spam
        for file_path in model_file_path:
            logging.debug(f"  - {file_path}")
        
        # Create and run investigation
        im = InvestigationPdbe(model_file_path, investigation_id, output_path, json_path)
        im.pre_run()
        im.run()
        
        logging.info(f"Investigation processing completed successfully")
        
    except Exception as e:
        logging.error(f"Investigation processing failed: {e}")
        raise

def parse_csv(csv_file: str) -> Dict[str, List[str]]:
    """
    Parse CSV file with improved validation and error handling.
    
    Args:
        csv_file: Path to CSV file with GROUP_ID and ENTRY_ID columns
        
    Returns:
        Dictionary mapping group IDs to lists of entry IDs
        
    Raises:
        ValueError: If CSV file is invalid or missing required columns
    """
    if not os.path.exists(csv_file):
        raise ValueError(f"CSV file not found: {csv_file}")
    
    group_data = {}
    required_columns = {"GROUP_ID", "ENTRY_ID"}
    
    try:
        with open(csv_file, 'r', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file, delimiter=",")
            
            # Validate required columns exist
            if not required_columns.issubset(set(csv_reader.fieldnames)):
                missing = required_columns - set(csv_reader.fieldnames)
                raise ValueError(f"CSV missing required columns: {missing}")
            
            row_count = 0
            for row_num, row in enumerate(csv_reader, start=2):  # Start at 2 for header
                try:
                    group_id = row["GROUP_ID"].strip()
                    entry_id = row["ENTRY_ID"].strip()
                    
                    if not group_id or not entry_id:
                        logging.warning(f"Empty values in row {row_num}: GROUP_ID='{group_id}', ENTRY_ID='{entry_id}'")
                        continue
                    
                    if group_id in group_data:
                        group_data[group_id].append(entry_id)
                    else:
                        group_data[group_id] = [entry_id]
                    
                    row_count += 1
                    
                except KeyError as e:
                    logging.error(f"Missing column in row {row_num}: {e}")
                except Exception as e:
                    logging.error(f"Error processing row {row_num}: {e}")
            
            logging.info(f"Parsed {row_count} entries into {len(group_data)} groups")
            
    except Exception as e:
        raise ValueError(f"Error reading CSV file {csv_file}: {e}")
    
    if not group_data:
        raise ValueError("No valid data found in CSV file")
    
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