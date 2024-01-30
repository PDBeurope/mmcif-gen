import gemmi
import sys
import logging
from openbabel import pybel
from output_grabber import OutputGrabber

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    

def smiles_to_inchikey_openbabel(smiles):
    out_stderr = OutputGrabber(sys.stderr)
    with out_stderr:
        mol = pybel.readstring("smi", smiles)
        inchikey = mol.write("inchikey").strip()
        logging.debug(f"SMILE: {smiles}")
        logging.debug(f"INCHI: {inchikey}")
        logging.debug(f"=================================")
        return inchikey

def process_mmcif_files(investigation_cif, sf_file_cif):
    sf_file = gemmi.cif.read(sf_file_cif)
    investigation = gemmi.cif.read(investigation_cif)

    inchi_keys = set()
    logging.info("Finding Smile string from sf file and converting them to Inchi keys")
    for block in sf_file:
        details_item = block.find_value("_diffrn.details")
        
        if details_item:
            smiles_string = details_item.split()[-1][:-1]
            inchi_keys.add(smiles_to_inchikey_openbabel(smiles_string))
    logging.info(f"Found and converted {len(inchi_keys)} smiles strings to Inchi key")

    logging.info("Finding existing Inchi keys in Investigation file ")
    for block_a in investigation:
        existing_inchis_in_investigation = block_a.get_mmcif_category("_pdbx_fraghub_investigation_fraglib_component")["inchi_descriptor"]
        existing_inchis = set(existing_inchis_in_investigation)

    logging.info(f"Found {len(existing_inchis)} Inchi keys in Investigation file")
    
    repeats = existing_inchis.intersection(inchi_keys)
    if repeats:
        logging.info(f"{len(repeats)} Inchi keys in sf file already exist in investigation files:")
        for inchi in repeats:
            logging.debug(inchi)
    else:
        logging.info("No Overlapping Inchi keys between sf file and investigation file")

    inchi_keys_to_add = inchi_keys - existing_inchis

    # Writing new inchis in investigation file (4 categories)
    fraglib_category = block_a.find_mmcif_category("_pdbx_fraghub_investigation_fraglib_component")
    fraglib_columns = {name: i for i, name in enumerate(fraglib_category.tags)}
    existing_fraglib_len = len(fraglib_category)

    component_mix_category = block_a.find_mmcif_category("_pdbx_fraghub_investigation_frag_component_mix")
    component_mix_columns = {name: i for i, name in enumerate(component_mix_category.tags)}
    existing_highest_id = int(max(block_a.get_mmcif_category("_pdbx_fraghub_investigation_frag_component_mix")["id"], key=int))

    existing_mix_category_len = len(component_mix_category)

    screening_exp_category = block_a.find_mmcif_category("_pdbx_fraghub_investigation_screening_exp")
    screening_exp_columns = {name: i for i, name in enumerate(screening_exp_category.tags)}
    screening_exp_category_len = len(screening_exp_category)
    screening_exp_template =  list(screening_exp_category[0])

    screening_result_category = block_a.find_mmcif_category("_pdbx_fraghub_investigation_screening_result")
    screening_result_columns = {name: i for i, name in enumerate(screening_result_category.tags)}
    screening_result_category_len = len(screening_result_category)
    screening_result_template =  list(screening_result_category[0])

    for index, inchi in enumerate(inchi_keys_to_add):
        inchi_index = str(index+existing_fraglib_len+1)
        row = [None] * fraglib_category.width()
        row[fraglib_columns["_pdbx_fraghub_investigation_fraglib_component.inchi_descriptor"]] = inchi
        row[fraglib_columns["_pdbx_fraghub_investigation_fraglib_component.id"]] = inchi_index
        row = gemmi.cif.quote_list(row)
        fraglib_category.append_row(row)


        row = [None]*component_mix_category.width()
        component_mix_index = str(existing_highest_id+index+1)
        row[component_mix_columns["_pdbx_fraghub_investigation_frag_component_mix.id"]]= component_mix_index
        row[component_mix_columns["_pdbx_fraghub_investigation_frag_component_mix.fraglib_component_id"]]= inchi_index
        row = gemmi.cif.quote_list(row)
        component_mix_category.append_row([component_mix_index, inchi_index])


        row = screening_exp_template
        screening_exp_index= str(index+screening_exp_category_len+1)
        row[screening_exp_columns["_pdbx_fraghub_investigation_screening_exp.frag_component_mix_id"]]= component_mix_index
        row[screening_exp_columns["_pdbx_fraghub_investigation_screening_exp.screening_exp_id"]]= screening_exp_index
        row = gemmi.cif.quote_list(row)
        screening_exp_category.append_row(row)

        row = screening_result_template
        screening_result_index= str(index+screening_result_category_len+1)

        row[screening_result_columns["_pdbx_fraghub_investigation_screening_result.screening_exp_id"]]= screening_exp_index
        row[screening_result_columns["_pdbx_fraghub_investigation_screening_result.result_id"]]= screening_result_index
        row[screening_result_columns["_pdbx_fraghub_investigation_screening_result.outcome"]]= "miss"
        row[screening_result_columns["_pdbx_fraghub_investigation_screening_result.fraglib_component_id"]] = component_mix_index #should this be ?
        row[screening_result_columns["_pdbx_fraghub_investigation_screening_result.outcome_description"]] = "Fragment Unobserved"
        row = gemmi.cif.quote_list(row)
        screening_result_category.append_row(row)


    investigation.write_file("test_out_investigation.cif")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <investigation-cif> <sf-file>")
        sys.exit(1)
    investigation_cif = sys.argv[1]
    sf_file = sys.argv[2]

    process_mmcif_files(investigation_cif, sf_file)
