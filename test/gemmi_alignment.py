import gemmi
from pathlib import Path

# Block.write_category behaves differently than category.append_row (with regards to quotes)
# gemmi.cif.quote adds quotes to strings where necessary
# It also adds quotes to the "?"
# gemmi.cif.quote_list quotes a list of string. 
# Unlike the quote function, it can take a None value, and return unquoted "?"



test_cif = Path('./data/test_gemmi.cif')
gemmi_cif = gemmi.cif.read(str(test_cif))

for block_a in gemmi_cif:
    category = block_a.find_mmcif_category("_audit_author")
    # columns = {name: i for i, name in enumerate(category.tags)}
    # print("Columns: "+str(columns))
    # category_len = len(category)
    one_value = gemmi.cif.quote("Name something")
    category.append_row([one_value, "4"])
    # The output does not have quotes, w
write_options = gemmi.cif.WriteOptions()
write_options.align_loops = 50
write_options.align_pairs = 50





block = gemmi_cif.sole_block()
loop = block.find_loop("_audit_author")
item_loop = block.find_loop_item("_audit_author.name")


import pdb
pdb.set_trace()
gemmi_cif.write_file("./data/test_gemmi_out.cif", write_options)


