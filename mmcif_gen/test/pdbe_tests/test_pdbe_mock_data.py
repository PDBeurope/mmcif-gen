"""
Mock data generators and fixtures for PDBe testing.

This module provides utilities to generate realistic test data for mmCIF files,
database records, and other test scenarios.
"""

import tempfile
import os
import json
from typing import Dict, List, Any


class MockmmCIFGenerator:
    """Generator for mock mmCIF file content."""
    
    @staticmethod
    def generate_basic_mmcif(pdb_id: str = "1ABC", entity_count: int = 2) -> str:
        """Generate realistic mmCIF content based on real PDB examples."""
        # Realistic sequences from actual PDB structures
        realistic_sequences = [
            "MEVKREHWATRLGLILAMAGNAVGLGNFLRFPVQAAENGGGAFMIPYIIAFLLVGIPLMWIEWAMGRYGGAQGHGTTPAIFYLLWRNRFAKILGVFGLWIPLVVAIYYVYIESWTLGFAIKFLVGLVPEPPPNATDPDSILRPFKEFLYSYIGVPKGDEPILKPSLFAYIVFLITMFINVSILIRGISKGIERFAKIAMPTLFILAVFLVIRVFLLETPNGTAADGLNFLWTPDFEKLKDPGVWIAAVGQIFFTLSLGFGAIITYASYVRKDQDIVLSGLTAATLNEKAEVILGGSISIPAAVAFFGVANAVAIAKAGAFNLGFITLPAIFSQTAGGTFLGFLWFFLLFFAGLTSSIAIMQPMIAFLEDELKLSRKHAVLWTAAIVFFSAHLVMFLNKSLDEMDFWAGTIGVVFFGLTELIIFFWIFGADKAWEEINRGGIIKVPRIYYYVMRYITPAFLAVLLVVWAREYIPKIMEETHHWTVWITRFYIIGLFLFLTFLVFLAERRRNHESAGTLVPR",
            "AGLRKMAQPSGVVEKCIVRVCYGNMALNGLWLGDTVMCPRHVIASSTTSTIDYDYALSVLRLHNFSISSGNVFLGVVGVTMRGALLQIKVNQNNVHTPKYTYRTVRPGESFNILACYDGAAAGVYGVNMRSNYTIRGSFINGAAGSPGYNINNGTVEFCYLHQLELGSGCHVGSDLDGVMYGGYEDQPTLQVEGASSLFTENVLAFLYAALINGSTWWLSSSRIAVDRFNEWAVHNGMTTVVNTDCFSILAAKTGVDVQRLLASIQSLHKNFGGKQILGYTSLTDEFTTGEVIRQMYGVHHHHHH"
        ]
        
        realistic_ligands = ["ATP", "NAD", "HEM", "ZN", "MG", "NA", "MSE"]
        realistic_descriptions = ["Leucine transporter", "3C-Like protease", "DNA-binding protein"]
        
        content = f"""data_{pdb_id}
#
_entry.id {pdb_id}
#
_database_2.database_id PDB
_database_2.database_code {pdb_id}
#
loop_
_entity.id
_entity.type
_entity.src_method
_entity.pdbx_description
_entity.formula_weight
_entity.pdbx_number_of_molecules
"""
        
        # Add entities with realistic data
        polymer_count = 0
        for i in range(1, entity_count + 1):
            if i <= max(1, entity_count * 0.7):  # Most entities are polymers
                entity_type = "polymer"
                src_method = "man" if i % 2 == 0 else "nat"
                description = realistic_descriptions[polymer_count % len(realistic_descriptions)]
                weight = "58077.438" if i == 1 else "33263.578"
                molecules = "2" if i == 1 else "1"
                polymer_count += 1
            else:
                entity_type = "non-polymer"
                src_method = "syn"
                ligand = realistic_ligands[(i-1) % len(realistic_ligands)]
                description = f"'{ligand}'"
                weight = "22.990" if ligand == "NA" else "196.106"
                molecules = "4" if ligand == "NA" else "2"
            
            content += f"{i} {entity_type} {src_method} {description} {weight} {molecules}\n"
        
        # Add polymer sequences for polymer entities
        if entity_count >= 1:
            content += """#
loop_
_entity_poly.entity_id
_entity_poly.type
_entity_poly.nstd_linkage
_entity_poly.nstd_monomer
_entity_poly.pdbx_seq_one_letter_code
_entity_poly.pdbx_seq_one_letter_code_can
_entity_poly.pdbx_strand_id
"""
            for i in range(1, min(polymer_count + 1, entity_count + 1)):
                if i <= max(1, entity_count * 0.7):
                    seq_idx = (i - 1) % len(realistic_sequences)
                    sequence = realistic_sequences[seq_idx][:100]  # Truncate for readability
                    sequence_can = sequence.replace("(MSE)", "M")
                    strand_id = chr(64 + i)  # A, B, C, etc.
                    content += f"{i} 'polypeptide(L)' no no\n;{sequence}\n;\n;{sequence_can}\n;\n{strand_id}\n"
        
        # Add non-polymer data
        nonpoly_entities = [i for i in range(1, entity_count + 1) if i > max(1, entity_count * 0.7)]
        if nonpoly_entities:
            content += """#
loop_
_pdbx_entity_nonpoly.entity_id
_pdbx_entity_nonpoly.name
_pdbx_entity_nonpoly.comp_id
"""
            for i in nonpoly_entities:
                ligand = realistic_ligands[(i-1) % len(realistic_ligands)]
                name = f"'{ligand}'" if ligand in ["NA"] else ligand
                content += f"{i} {name} {ligand}\n"
        
        # Add source information
        content += """#
_entity_src_gen.entity_id                          1
_entity_src_gen.pdbx_gene_src_scientific_name      'Aquifex aeolicus'
_entity_src_gen.pdbx_gene_src_ncbi_taxonomy_id     63363
_entity_src_gen.pdbx_host_org_scientific_name      'Escherichia coli'
_entity_src_gen.pdbx_host_org_ncbi_taxonomy_id     562
_entity_src_gen.pdbx_host_org_vector_type          plasmid
_entity_src_gen.plasmid_name                       pet16b
"""
        return content
    
    @staticmethod
    def generate_complex_mmcif(pdb_id: str = "2XYZ") -> str:
        """Generate complex mmCIF with multiple entity types based on real examples."""
        return f"""data_{pdb_id}
#
_entry.id {pdb_id}
#
_database_2.database_id PDB
_database_2.database_code {pdb_id}
#
loop_
_entity.id
_entity.type
_entity.src_method
_entity.pdbx_description
_entity.formula_weight
_entity.pdbx_number_of_molecules
_entity.pdbx_ec
_entity.pdbx_mutation
_entity.pdbx_fragment
1 polymer man 'PEDV 3C-Like protease' 33263.578 2 ? C144A 'UNP RESIDUES 2998-3296'
2 polymer syn 'peptide substrate SAVLQSGF' 1195.369 1 ? ? ?
3 non-polymer syn 'SODIUM ION' 22.990 4 ? ? ?
4 non-polymer syn SELENOMETHIONINE 196.106 2 ? ? ?
5 water nat water 18.015 267 ? ? ?
#
loop_
_entity_poly.entity_id
_entity_poly.type
_entity_poly.nstd_linkage
_entity_poly.nstd_monomer
_entity_poly.pdbx_seq_one_letter_code
_entity_poly.pdbx_seq_one_letter_code_can
_entity_poly.pdbx_strand_id
_entity_poly.pdbx_target_identifier
1 'polypeptide(L)' no no
;AGLRKMAQPSGVVEKCIVRVCYGNMALNGLWLGDTVMCPRHVIASSTTSTIDYDYALSVLRLHNFSISSGNVFLGVVGVT
MRGALLQIKVNQNNVHTPKYTYRTVRPGESFNILACYDGAAAGVYGVNMRSNYTIRGSFINGAAGSPGYNINNGTVEFCY
LHQLELGSGCHVGSDLDGVMYGGYEDQPTLQVEGASSLFTENVLAFLYAALINGSTWWLSSSRIAVDRFNEWAVHNGMTT
VVNTDCFSILAAKTGVDVQRLLASIQSLHKNFGGKQILGYTSLTDEFTTGEVIRQMYGVHHHHHH
;
;AGLRKMAQPSGVVEKCIVRVCYGNMALNGLWLGDTVMCPRHVIASSTTSTIDYDYALSVLRLHNFSISSGNVFLGVVGVT
MRGALLQIKVNQNNVHTPKYTYRTVRPGESFNILACYDGAAAGVYGVNMRSNYTIRGSFINGAAGSPGYNINNGTVEFCY
LHQLELGSGCHVGSDLDGVMYGGYEDQPTLQVEGASSLFTENVLAFLYAALINGSTWWLSSSRIAVDRFNEWAVHNGMTT
VVNTDCFSILAAKTGVDVQRLLASIQSLHKNFGGKQILGYTSLTDEFTTGEVIRQMYGVHHHHHH
;
A,B ?
2 'polypeptide(L)' no no
;SAVLQSGF
;
;SAVLQSGF
;
C ?
#
loop_
_pdbx_entity_nonpoly.entity_id
_pdbx_entity_nonpoly.name
_pdbx_entity_nonpoly.comp_id
3 'SODIUM ION' NA
4 SELENOMETHIONINE MSE
#
_entity_src_gen.entity_id                          1
_entity_src_gen.pdbx_gene_src_scientific_name      'Aquifex aeolicus'
_entity_src_gen.pdbx_gene_src_ncbi_taxonomy_id     63363
_entity_src_gen.pdbx_host_org_scientific_name      'Escherichia coli'
_entity_src_gen.pdbx_host_org_ncbi_taxonomy_id     562
_entity_src_gen.pdbx_host_org_vector_type          plasmid
_entity_src_gen.plasmid_name                       pet16b
"""
    
    @staticmethod
    def generate_malformed_mmcif(pdb_id: str = "BAD1") -> str:
        """Generate malformed mmCIF for error testing."""
        return f"""data_{pdb_id}
#
_entry.id {pdb_id}
# Missing database_2 section
#
loop_
_entity.id
_entity.type
# Missing src_method column
1 polymer
2 'invalid type with spaces'
# Incomplete data
"""


class MockDatabaseGenerator:
    """Generator for mock database records."""
    
    @staticmethod
    def generate_entity_records(count: int = 5) -> List[Dict[str, Any]]:
        """Generate realistic mock entity records based on real mmCIF examples."""
        # Real protein sequences from PDB examples
        realistic_sequences = [
            "MEVKREHWATRLGLILAMAGNAVGLGNFLRFPVQAAENGGGAFMIPYIIAFLLVGIPLMWIEWAMGRYGGAQGHGTTPAIFYLLWRNRFAKILGVFGLWIPLVVAIYYVYIESWTLGFAIKFLVGLVPEPPPNATDPDSILRPFKEFLYSYIGVPKGDEPILKPSLFAYIVFLITMFINVSILIRGISKGIERFAKIAMPTLFILAVFLVIRVFLLETPNGTAADGLNFLWTPDFEKLKDPGVWIAAVGQIFFTLSLGFGAIITYASYVRKDQDIVLSGLTAATLNEKAEVILGGSISIPAAVAFFGVANAVAIAKAGAFNLGFITLPAIFSQTAGGTFLGFLWFFLLFFAGLTSSIAIMQPMIAFLEDELKLSRKHAVLWTAAIVFFSAHLVMFLNKSLDEMDFWAGTIGVVFFGLTELIIFFWIFGADKAWEEINRGGIIKVPRIYYYVMRYITPAFLAVLLVVWAREYIPKIMEETHHWTVWITRFYIIGLFLFLTFLVFLAERRRNHESAGTLVPR",
            "AGLRKMAQPSGVVEKCIVRVCYGNMALNGLWLGDTVMCPRHVIASSTTSTIDYDYALSVLRLHNFSISSGNVFLGVVGVTMRGALLQIKVNQNNVHTPKYTYRTVRPGESFNILACYDGAAAGVYGVNMRSNYTIRGSFINGAAGSPGYNINNGTVEFCYLHQLELGSGCHVGSDLDGVMYGGYEDQPTLQVEGASSLFTENVLAFLYAALINGSTWWLSSSRIAVDRFNEWAVHNGMTTVVNTDCFSILAAKTGVDVQRLLASIQSLHKNFGGKQILGYTSLTDEFTTGEVIRQMYGVHHHHHH",
            "SAVLQSGF",  # Short peptide substrate
            "MKLLVLGLGLGLGLGLGLGL",  # Membrane protein fragment
            "ACDEFGHIKLMNPQRSTVWY"   # Standard amino acid sequence
        ]
        
        # Realistic chemical components from PDB
        realistic_ligands = ["ATP", "ADP", "GTP", "NAD", "FAD", "HEM", "ZN", "MG", "CA", "FE", "NA", "CL", "MSE", "HOH"]
        
        # Realistic organism data
        realistic_organisms = [
            ("Escherichia coli", "562"),
            ("Homo sapiens", "9606"), 
            ("Aquifex aeolicus", "63363"),
            ("Thermus thermophilus", "274"),
            ("Saccharomyces cerevisiae", "4932"),
            ("Mus musculus", "10090"),
            ("synthetic construct", "32630")
        ]
        
        # Realistic protein descriptions
        protein_descriptions = [
            "Leucine transporter",
            "3C-Like protease", 
            "DNA-binding protein",
            "Membrane transport protein",
            "Kinase domain",
            "Transcription factor",
            "Enzyme complex subunit"
        ]
        
        records = []
        for i in range(1, count + 1):
            # Determine entity type with realistic distribution
            if i <= count * 0.6:  # 60% proteins
                entity_type = "polymer"
                src_method = "man" if i % 3 == 0 else "nat"  # Mix of natural and engineered
                seq_idx = (i - 1) % len(realistic_sequences)
                sequence = realistic_sequences[seq_idx]
                sequence_can = sequence.replace("(MSE)", "M")  # Convert modified residues
                chem_comp_id = ""
                poly_type = "polypeptide(L)"
                description = protein_descriptions[(i - 1) % len(protein_descriptions)]
            elif i <= count * 0.85:  # 25% non-polymer ligands
                entity_type = "non-polymer"
                src_method = "syn"
                sequence = ""
                sequence_can = ""
                chem_comp_id = realistic_ligands[(i - 1) % len(realistic_ligands)]
                poly_type = ""
                description = f"{chem_comp_id} ligand"
            else:  # 15% water
                entity_type = "water"
                src_method = "nat"
                sequence = ""
                sequence_can = ""
                chem_comp_id = "HOH"
                poly_type = ""
                description = "water"
            
            # Select realistic organism
            org_idx = (i - 1) % len(realistic_organisms)
            organism_scientific, ncbi_taxonomy_id = realistic_organisms[org_idx]
            
            # Generate realistic PDB ID (4 characters: digit + 3 alphanumeric)
            pdb_id = f"{i % 10}{chr(65 + (i % 26))}{chr(65 + ((i * 2) % 26))}{chr(65 + ((i * 3) % 26))}"
            
            records.append({
                "ordinal": i,
                "pdb_id": pdb_id,
                "file_name": f"{pdb_id.lower()}.cif",
                "model_file_no": "",
                "entity_id": str(i),
                "type": entity_type,
                "seq_one_letter_code": sequence,
                "seq_one_letter_code_can": sequence_can,
                "chem_comp_id": chem_comp_id,
                "src_method": src_method,
                "description": description,
                "poly_type": poly_type,
                "organism_scientific": organism_scientific,
                "ncbi_taxonomy_id": ncbi_taxonomy_id
            })
        return records


class MockFileGenerator:
    """Generator for mock files and directories."""
    
    def __init__(self):
        self.temp_dirs = []
    
    def create_test_environment(self) -> str:
        """Create a complete test environment with files."""
        temp_dir = tempfile.mkdtemp()
        self.temp_dirs.append(temp_dir)
        
        # Create CIF files
        cif_dir = os.path.join(temp_dir, "cif_files")
        os.makedirs(cif_dir)
        
        # Create valid CIF files
        for i, pdb_id in enumerate(["1ABC", "2DEF", "3GHI"], 1):
            cif_file = os.path.join(cif_dir, f"{pdb_id.lower()}.cif")
            with open(cif_file, 'w') as f:
                f.write(MockmmCIFGenerator.generate_basic_mmcif(pdb_id, i))
        
        # Create complex CIF file
        complex_cif = os.path.join(cif_dir, "complex.cif")
        with open(complex_cif, 'w') as f:
            f.write(MockmmCIFGenerator.generate_complex_mmcif())
        
        # Create malformed CIF file
        bad_cif = os.path.join(cif_dir, "malformed.cif")
        with open(bad_cif, 'w') as f:
            f.write(MockmmCIFGenerator.generate_malformed_mmcif())
        
        # Create operation JSON file
        operation_file = os.path.join(temp_dir, "operation.json")
        operation_data = {
            "investigation_type": "ligand_screening",
            "version": "1.0",
            "parameters": {
                "batch_size": 100,
                "timeout": 30
            }
        }
        with open(operation_file, 'w') as f:
            json.dump(operation_data, f, indent=2)
        
        # Create CSV file for group testing
        csv_file = os.path.join(temp_dir, "test_groups.csv")
        csv_content = """GROUP_ID,ENTRY_ID
group1,1ABC
group1,2DEF
group2,3GHI
group2,4JKL
group3,5MNO
"""
        with open(csv_file, 'w') as f:
            f.write(csv_content)
        
        # Create config file
        config_file = os.path.join(temp_dir, "config.json")
        config_data = {
            "batch_size": 50,
            "download_timeout": 60,
            "max_retries": 2,
            "log_level": "DEBUG"
        }
        with open(config_file, 'w') as f:
            json.dump(config_data, f, indent=2)
        
        # Create output directory
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(output_dir)
        
        return temp_dir
    
    def cleanup(self):
        """Clean up all created temporary directories."""
        import shutil
        for temp_dir in self.temp_dirs:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
        self.temp_dirs.clear()


class MockResponseGenerator:
    """Generator for mock HTTP responses."""
    
    @staticmethod
    def create_successful_response(content: bytes = b"fake gzipped content") -> object:
        """Create a mock successful HTTP response."""
        from unittest.mock import Mock
        response = Mock()
        response.status_code = 200
        response.content = content
        return response
    
    @staticmethod
    def create_failed_response(status_code: int = 404) -> object:
        """Create a mock failed HTTP response."""
        from unittest.mock import Mock
        response = Mock()
        response.status_code = status_code
        response.content = b""
        return response
    
    @staticmethod
    def create_timeout_response() -> object:
        """Create a mock response that raises a timeout."""
        from unittest.mock import Mock
        import requests
        response = Mock()
        response.side_effect = requests.Timeout("Request timed out")
        return response


class TestDataValidator:
    """Utilities for validating test data and results."""
    
    @staticmethod
    def validate_entity_record(record: Dict[str, Any]) -> bool:
        """Validate that an entity record has all required fields."""
        required_fields = [
            "ordinal", "pdb_id", "file_name", "entity_id", "type",
            "seq_one_letter_code", "seq_one_letter_code_can",
            "chem_comp_id", "src_method", "description", "poly_type",
            "organism_scientific", "ncbi_taxonomy_id"
        ]
        
        return all(field in record for field in required_fields)
    
    @staticmethod
    def validate_batch_consistency(batch: List[Dict[str, Any]]) -> bool:
        """Validate that all records in a batch are consistent."""
        if not batch:
            return True
        
        # Check that all records have the same structure
        first_record_keys = set(batch[0].keys())
        return all(set(record.keys()) == first_record_keys for record in batch)
    
    @staticmethod
    def count_entity_types(records: List[Dict[str, Any]]) -> Dict[str, int]:
        """Count entities by type for validation."""
        type_counts = {}
        for record in records:
            entity_type = record.get("type", "unknown")
            type_counts[entity_type] = type_counts.get(entity_type, 0) + 1
        return type_counts


# Example usage and test data constants based on real PDB examples
SAMPLE_PDB_IDS = ["3USO", "4ZUH", "1XXX", "7DY3", "9IKU"]  # Real PDB IDs from examples
INVALID_PDB_IDS = ["invalid", "ABCD", "123", "", "1AB@"]
MIXED_PDB_IDS = SAMPLE_PDB_IDS + INVALID_PDB_IDS

# Realistic CSV content with actual PDB IDs
SAMPLE_CSV_CONTENT = """GROUP_ID,ENTRY_ID
transporter_group,3USO
transporter_group,1XXX
protease_group,4ZUH
protease_group,7DY3
kinase_group,9IKU
"""

# Realistic configuration based on actual PDB processing requirements
SAMPLE_CONFIG = {
    "batch_size": 100,
    "download_timeout": 30,
    "max_retries": 3,
    "validate_mmcif_format": True,
    "cleanup_temp_files": True,
    "log_level": "INFO"
}

# Realistic organism data from actual PDB structures
REALISTIC_ORGANISMS = [
    ("Escherichia coli", "562"),
    ("Homo sapiens", "9606"), 
    ("Aquifex aeolicus", "63363"),
    ("Thermus thermophilus", "274"),
    ("Saccharomyces cerevisiae", "4932"),
    ("Mus musculus", "10090"),
    ("synthetic construct", "32630")
]

# Common chemical components found in PDB structures
REALISTIC_LIGANDS = [
    "ATP", "ADP", "GTP", "GDP", "NAD", "NADH", "FAD", "FMN",
    "HEM", "ZN", "MG", "CA", "FE", "NA", "CL", "K", "MN",
    "MSE", "HOH", "SO4", "PO4", "GOL", "EDO"
]
