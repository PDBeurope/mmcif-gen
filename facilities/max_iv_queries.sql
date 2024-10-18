

-- Intelligent Splitting
WITH RECURSIVE split(protein_batch_id, protein_batch_comp_id, str) 
    AS (
        SELECT distinct(protein_batch_id), '', protein_batch_comp_id ||' ' FROM denormalized_data
        UNION ALL SELECT
        protein_batch_id,
        substr(str, 0, instr(str, ' ')),
        substr(str, instr(str, ' ')+1)
        FROM split WHERE str!=''
    ) 
SELECT protein_batch_id, protein_batch_comp_id
FROM split
WHERE protein_batch_comp_id!=''

--- All chem_comp_codes


WITH RECURSIVE split1(crystal_screen_id, code, str) 
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
chem_code as (
    Select code from split1
    UNION
    Select code from split2
    UNION
    Select code from split3
    )
SELECT DISTINCT(code) from chem_code

--- get chem details along with chem_comp_id

WITH RECURSIVE split1(crystal_screen_id, code, str) 
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


--- query to extract protein sequences without prefixes, from protein_batch_table
WITH RECURSIVE split(protein_batch_uniprot_id, seq, str) 
    AS (
        SELECT distinct(protein_batch_uniprot_id), '', protein_batch_sequence ||'\n' as code FROM protein_batch_table
        UNION ALL SELECT
        protein_batch_uniprot_id,
        substr(str, 0, instr(str, '\n')),
        substr(str, instr(str, '\n')+1)
        FROM split WHERE str!=''
    )
    SELECT seq, protein_batch_uniprot_id,'polypeptide(L)' as type, 'man' as method, 'UNP' as db FROM split WHERE length(seq)> 7

--- nonpoly descript:

SELECT  protein_batch_comp_id, crystal_screen_chem_comp_ids, cryo_chem_comp_code from denormalized_data 
group by protein_batch_comp_id, crystal_screen_chem_comp_ids, cryo_chem_comp_code

--- poly descript
-- It should be distinct "protein_batch_sequence" present in protein_batch_table


--_pdbx_investigation_sample.sample_id 
SELECT  protein_batch_sequence, protein_batch_comp_id,  crystal_screen_chem_comp_ids, cryo_chem_comp_code from denormalized_data 
group by protein_batch_comp_id, crystal_screen_chem_comp_ids, cryo_chem_comp_code, protein_batch_sequence

--with concat

SELECT  protein_batch_sequence, COALESCE(protein_batch_comp_id, '') || ' ' || COALESCE(crystal_screen_chem_comp_ids,'') || ' ' || COALESCE(cryo_chem_comp_code,'') from denormalized_data 
group by protein_batch_comp_id, crystal_screen_chem_comp_ids, cryo_chem_comp_code, protein_batch_sequence