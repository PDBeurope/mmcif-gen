from mmcif_gen.investigation_engine import InvestigationEngine
from mmcif_gen.investigation_io import SqliteReader
from typing import List
import sys
import logging
import argparse

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

class InvestigationMaxIV(InvestigationEngine):
        
    def __init__(self, sqlite_path: str, investigation_id: str, output_path: str, maxiv_investigation_json: str="./operations/maxiv/maxiv_investigation.json") -> None:
        logging.info("Instantiating MaxIV Investigation subclass")
        self.reader = SqliteReader(sqlite_path)
        self.operation_file_json = maxiv_investigation_json
        super().__init__(investigation_id, output_path)

    def pre_run(self) -> None:
        logging.info("Pre-running")
        self.create_denormalized_table()
        self.create_entity_table()
        self.add_descript_categories()
        super().pre_run()

    def create_denormalized_table(self) -> None:
        logging.info("Creating denormalized table")
        query = "DROP TABLE IF EXISTS denormalized_data"
        self.reader.sql_execute(query)
        # Remove rows where S.data_collection_type is 'dummy', as no crystal is in the beam.
        query ='''CREATE TABLE denormalized_data AS 
        SELECT 
            S.dataset_id, S.session, S.data_collection_date, S.data_collection_outcome,
            A.mounted_crystal_id, A.cryo_chem_comp_code,
            B.crystal_plate_id, B.marked_crystal_id, B.crystal_screen_condition_id,
            C.soak_plate_id,
            E.crystal_plate_id, E.protein_batch_id,
            F.protein_batch_sequence, F.protein_batch_comp_id, F.protein_batch_uniprot_id, F.protein_acronym,
            G.solvent, G.compound_code, G.compound_batch_code, G.library_name,
            H.smiles, H.inchi, H.chemical_name, H.formula, H.formula_weight, H.cas,
            I.protein_name, I.proposal_number,
            J.crystal_screen_id, J.crystal_screen_chem_comp_ids
        FROM xray_dataset_table S
        INNER JOIN mounted_crystals_table A on S.mounted_crystal_id = A.mounted_crystal_id
            INNER JOIN marked_crystals_table B on A.marked_crystal_id = B.marked_crystal_id
                LEFT JOIN soaked_crystals_table C on B.marked_crystal_id = C.marked_crystal_id 
                    INNER JOIN soak_plate_table D on C.soak_plate_id = D.soak_plate_id
                        INNER JOIN compound_batch_table G on G.compound_batch_code = D.compound_batch_code
                            INNER JOIN compound_table H on H.compound_code = G.compound_code
                INNER JOIN crystal_plate_table E on B.crystal_plate_id = E.crystal_plate_id
                    INNER JOIN protein_batch_table F on E.protein_batch_id = F.protein_batch_id
                    INNER JOIN project_table I on I.protein_acronym = F.protein_acronym
                INNER JOIN crystal_screen_condition_table J on B.crystal_screen_condition_id = J.crystal_screen_condition_id
                WHERE  G.library_name  NOT IN ('solvent test', 'SU','solvent_test','solvent_only','SU_FU_2023-12-07','solvent_tolerance_231211')
        '''
        self.reader.sql_execute(query)

    def create_entity_table(self) -> None:
        logging.info("Creating entity table")
        with self.reader.sqlite_db_connection() as cursor:
            query = "DROP TABLE IF EXISTS entities"
            cursor.execute(query)
            query = '''
                    CREATE TABLE entities (
                        entity_id TEXT,
                        type TEXT,
                        sequence TEXT,
                        chem_comp_id TEXT,
                        name TEXT,
                        formula TEXT,
                        formula_weight TEXT,
                        inchi TEXT,
                        uniprot_id TEXT,
                        sample_id INT,
                        campaign_id TEXT,
                        series_id TEXT
                )
            '''
            cursor.execute(query)

            chem_comp_query = '''WITH RECURSIVE split1(crystal_screen_id, code, str) 
                    AS (
                        SELECT distinct(crystal_screen_id), '', crystal_screen_chem_comp_ids ||' ' as code FROM denormalized_data
                        UNION ALL SELECT
                        crystal_screen_id,
                        substr(str, 0, instr(str, ' ')),
                        substr(str, instr(str, ' ')+1)
                        FROM split1 WHERE str!=''
                    ),
                split2(protein_batch_id, code, str) 
                    AS (
                        SELECT distinct(protein_batch_id), '', protein_batch_comp_id ||' ' as code FROM denormalized_data
                        UNION ALL SELECT
                        protein_batch_id,
                        substr(str, 0, instr(str, ' ')),
                        substr(str, instr(str, ' ')+1)
                        FROM split2 WHERE str!=''
                    ),
                split3(mounted_crystal_id, code, str) 
                    AS (
                        SELECT distinct(mounted_crystal_id), '', cryo_chem_comp_code ||' ' as code FROM denormalized_data
                        UNION ALL SELECT
                        mounted_crystal_id,
                        substr(str, 0, instr(str, ' ')),
                        substr(str, instr(str, ' ')+1)
                        FROM split3 WHERE str!=''
                    ), 
                combined as (
                    Select code from split1
                    UNION
                    Select code from split2
                    UNION
                    Select code from split3
                    )
                SELECT DISTINCT(code), c.name, c.formula, c.formula_weight, c.inchi from combined LEFT JOIN wwpdb_chem_comp_table c on c.chem_comp_code = combined.code where combined.code != '' 
            '''
            results = self.reader.sql_execute(chem_comp_query)

            for index, result in enumerate(results):
                insert_query = """
                        INSERT INTO entities
                        (entity_id, type, chem_comp_id,name, formula, formula_weight, inchi )
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """
                cursor.execute(
                        insert_query,
                        ( str(index+1),
                            "non-polymer",
                            result[0],
                            result[1],
                            result[2],
                            result[3],
                            result[4]
                        ),
                        )
            poly_query = '''
                        WITH RECURSIVE split(protein_batch_uniprot_id, seq, str) AS 
                        (SELECT distinct(protein_batch_uniprot_id), '', protein_batch_sequence ||'\n' as code 
                        FROM protein_batch_table 
                        UNION ALL 
                        SELECT protein_batch_uniprot_id, 
                        substr(str, 0, instr(str, '\n')), 
                        substr(str, instr(str, '\n')+1) 
                        FROM split WHERE str!='' ) 
                        SELECT seq, protein_batch_uniprot_id FROM split WHERE length(seq)> 7'''
            
            results = self.reader.sql_execute(poly_query)
            for index, result in enumerate(results):
                insert_query = """
                        INSERT INTO entities
                        (entity_id, sequence, type, uniprot_id )
                        VALUES (?, ?, ?, ?)
                    """
                cursor.execute(insert_query,
                                ( str(index+1),
                                    result[0],
                                    "polymer",
                                    result[1]
                                ),)

            

    def add_descript_categories(self):
        logging.info("Adding Descript categories")
        query = "DROP TABLE IF EXISTS descript"
        self.reader.sql_execute(query)
        query_creating_descript = '''
                                    CREATE TABLE descript (
                                    entity_id int,
                                    fragment_component_id int,
                                    fragment_component_mix int,
                                    dataset_id int,
                                    chem_comp text,
                                    sequence text,
                                    fragment_inchi text,
                                    poly_descript int,
                                    nonpoly_descript int,
                                    sample_id int)
                                    '''
        self.reader.sql_execute(query_creating_descript)

        query_nonpoly_descript = '''
                                    SELECT  protein_batch_comp_id, crystal_screen_chem_comp_ids, cryo_chem_comp_code from denormalized_data 
                                    group by protein_batch_comp_id, crystal_screen_chem_comp_ids, cryo_chem_comp_code
                                    '''
        nonpoly_result = self.reader.sql_execute(query_nonpoly_descript)

        query_poly_descript = "SELECT  entity_id, sequence FROM entities WHERE type='polymer'"
        poly_result = self.reader.sql_execute(query_poly_descript)

        query_poly_mapping = "SELECT distinct(protein_batch_sequence) FROM protein_batch_table"
        poly_mapping_result = self.reader.sql_execute(query_poly_mapping)

        chem_comp_entity_id_query = "SELECT entity_id, chem_comp_id FROM entities WHERE type='non-polymer' "
        nonpoly_mapping_result = self.reader.sql_execute(chem_comp_entity_id_query)

        query_sample = '''
                            SELECT  protein_batch_sequence, 
                            COALESCE(protein_batch_comp_id, '') || ' ' || COALESCE(crystal_screen_chem_comp_ids,'') || ' ' || COALESCE(cryo_chem_comp_code,'') 
                            FROM denormalized_data 
                            group by protein_batch_comp_id, crystal_screen_chem_comp_ids, cryo_chem_comp_code, protein_batch_sequence
                        '''
        sample_result = self.reader.sql_execute(query_sample)

        query_fragment = '''
                SELECT inchi, dataset_id from denormalized_data where inchi is not null and dataset_id is not NULL ORDER BY dataset_id
            '''
        fragment_result = self.reader.sql_execute(query_fragment)

        mapping = {}
        for row in nonpoly_mapping_result:
            mapping[row[1]] = row[0]
        
        with self.reader.sqlite_db_connection() as cursor:
            for sample_index, entities in enumerate(sample_result):
                non_poly_sample_entities = entities[1].split()
                for descript, chem_comp_codes in enumerate(nonpoly_result):
                    nonpoly_descript = []
                    for group in chem_comp_codes:
                        if group:
                            nonpoly_descript += group.split()
                    for chem_comp_id in nonpoly_descript:
                        sample_id = None
                        if chem_comp_id in non_poly_sample_entities:
                            sample_id = sample_index+1
                        insert_query = '''
                            INSERT INTO descript
                            (entity_id, chem_comp, nonpoly_descript, sample_id )
                            VALUES (?, ?, ?, ?)'''
                        cursor.execute(insert_query, (mapping[chem_comp_id],chem_comp_id, descript+1, sample_id))
                for seq in poly_result:
                    for  descript, row in enumerate(poly_mapping_result):
                        if seq[1] in row[0]:
                            sample_id = None
                            if seq[1] in entities[0]:
                                sample_id = sample_index+1
                            insert_query = '''
                                    INSERT INTO descript
                                    (entity_id, sequence, poly_descript, sample_id )
                                    VALUES (?, ?, ?, ?)'''
                            cursor.execute(insert_query, (seq[0],seq[1], descript+1, sample_id))

            previous_dataset_id = ""
            frag_component_mix = 0
            fragment_component_id_mapping = {}
            next_component_id = 1
            fragmix_data = []
            frag_data = []
            mix_for_index = []
            for index, fragment_data in enumerate(fragment_result):
                if fragment_data[1] != previous_dataset_id:
                    frag_component_mix += 1

                if fragment_data[0]  not in fragment_component_id_mapping:
                    fragment_component_id_mapping[fragment_data[0]] = next_component_id
                    next_component_id += 1

                fragment_component_id = fragment_component_id_mapping[fragment_data[0]]
                
                fragmix_data.append((frag_component_mix, fragment_component_id))
                mix_for_index.append(frag_component_mix)
                frag_data.append((fragment_data[0],fragment_data[1]))
                
                previous_dataset_id = fragment_data[1]

            # Finding all mixes:

            mix_to_fragments = {}
            for mix, fragment in fragmix_data:
                if mix not in mix_to_fragments:
                    mix_to_fragments[mix] = set()
                mix_to_fragments[mix].add(fragment)

            # Finding Unique mixes
            unique_fragments = {}
            mix_id = 0 
            for mix, fragments in mix_to_fragments.items():
                fragment_tuple = tuple(sorted(fragments))  # Sort and convert to tuple for hashability
                if fragment_tuple not in unique_fragments:
                    index_in_frag_data = mix_for_index.index(mix)
                    unique_fragments[fragment_tuple] = mix
                    mix_id += 1
                    for frag in fragment_tuple:
                        insert_query = '''
                                INSERT INTO descript
                                (fragment_component_id, fragment_component_mix, fragment_inchi, dataset_id)
                                VALUES (?, ?, ?, ?)'''
                        cursor.execute(insert_query, (frag, mix_id, frag_data[index_in_frag_data][0],frag_data[index_in_frag_data][1]))




def run(sqlite_path : str, investigation_id: str, output_path: str) -> None:
    im = InvestigationMaxIV(sqlite_path, investigation_id, output_path)
    im.pre_run()
    im.run()
    
def maxiv_subparser(subparsers, parent_parser):
    parser_maxiv = subparsers.add_parser("maxiv",help="Parameter requirements for investigation files from MAX IV data",parents=[parent_parser])
    parser_maxiv.add_argument(
        "-s",
        "--sqlite",
        help="Path to the Sqlite DB for the given investigation",
    )

def run_investigation_maxiv(args):
    if not args.sqlite:
        logging.error("Max IV facility requires path to --sqlite file")
        return 1
    run(args.sqlite, args.id, args.output_folder,args.json)


    