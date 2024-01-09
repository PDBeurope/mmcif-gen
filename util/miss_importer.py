import gemmi
import pybel
import sys

def smiles_to_inchikey_openbabel(smiles):
    mol = pybel.readstring("smi", smiles)
    inchikey = mol.write("inchikey").strip()
    return inchikey

def process_mmcif_files(investigation_cif, sf_file_cif):
    sf_file = gemmi.cif.read(sf_file_cif)
    investigation = gemmi.cif.read(investigation_cif)

    inchi_keys = set()
    # Finding all inchikeys from sf files
    for block in sf_file:
        details_item = block.find_value("_diffrn.details")
        
        if details_item:
            smiles_string = details_item.split()[-1][:-1]
            print(smiles_string)

            inchi_keys.add(smiles_to_inchikey_openbabel(smiles_string))

    # Finding existing inchis in investigation file
    for block_a in investigation:
        existing_inchis_in_investigation = block_a.get_mmcif_category("_pdbx_fraghub_investigation_fraglib_component")["inchi_descriptor"]
        existing_inchis = set(existing_inchis_in_investigation)
    
    # TODO: log overlapped inchi keys
    inchi_keys_to_add = inchi_keys - existing_inchis

    # Writing new inchis in investigation file (4 categories)
    fraglib_category = block_a.find_mmcif_category("_pdbx_fraghub_investigation_fraglib_component")
    existing_fraglib_len = len(fraglib_category)

    component_mix_category = block_a.find_mmcif_category("_pdbx_fraghub_investigation_frag_component_mix")
    existing_highest_id = int(max(block_a.get_mmcif_category("_pdbx_fraghub_investigation_frag_component_mix")["id"], key=int))

    existing_mix_category_len = len(component_mix_category)

    screening_exp_category = block_a.find_mmcif_category("_pdbx_fraghub_investigation_screening_exp")
    screening_exp_category_len = len(screening_exp_category)
    screening_exp_template =  list(screening_exp_category[0])

    screening_result_category = block_a.find_mmcif_category("_pdbx_fraghub_investigation_screening_result")
    screening_result_category_len = len(screening_result_category)
    screening_result_template =  list(screening_result_category[0])

    for index, inchi in enumerate(inchi_keys_to_add):
        inchi_index = str(index+existing_fraglib_len+1)
        fraglib_category.append_row(['?', inchi, '?', '?', '?', '?', '?', '?' ,inchi_index])

        component_mix_index = str(existing_highest_id+index+1)
        component_mix_category.append_row([component_mix_index, inchi_index])


        screening_exp_index= str(index+screening_exp_category_len+1)
        screening_exp_template[4] = component_mix_index
        screening_exp_template[7] = screening_exp_index
        screening_exp_category.append_row(screening_exp_template)

        screening_result_index= str(index+screening_result_category_len+1)
        screening_result_template[0] = screening_exp_index        
        # screening_result_template[0] = "?"
        screening_result_template[1] = screening_result_index
        screening_result_template[2] = "miss"
        screening_result_template[3] = "?"
        screening_result_template[5] = "Fragment Unobserved"
        screening_result_category.append_row(screening_result_template)


    investigation.write_file("test_out_investigation.cif")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <investigation-cif> <sf-file>")
        sys.exit(1)
    investigation_cif = sys.argv[1]
    sf_file = sys.argv[2]

    process_mmcif_files(investigation_cif, sf_file)
