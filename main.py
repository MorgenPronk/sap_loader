# %%
print("Importing libraries...")
import sys
import pandas as pd
import os
from openpyxl import load_workbook
from typing import NamedTuple, Any, Union, List, Dict
import logging
import json
import shutil
from tqdm import tqdm
from functools import lru_cache
from datetime import datetime

@lru_cache(maxsize=1)
def _load_config():
    with open(os.path.join("data", "config.json"), 'r') as f:
        return json.load(f)

# %%
def config_logging(filepath):

    filepath = os.path.abspath(filepath)

    if not os.path.exists(filepath):
        with open(filepath, 'w') as f:
            f.write("")

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        filename=filepath,
        filemode='a',
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def _progress_path(loadsheet_path: str) -> str:
    return loadsheet_path + ".progress.json"

def load_progress(loadsheet_path: str):
    try:
        with open(_progress_path(loadsheet_path), "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return None
    
def save_progress(loadsheet_path: str, equipment_start_index: int, row_start: int, current_row_offset: int):
    data = {
        "equipment_start_index": equipment_start_index,
        "row_start": row_start,
        "current_row": row_start + current_row_offset
    }
    with open(_progress_path(loadsheet_path), "w") as f:
        json.dump(data, f)

def reset_output_from_template(template_path: str, output_path: str, backup=True):
    """
    Start fresh : optionally back up existing output file, then copy the tmplate to output path
    """
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    if os.path.isfile(output_path) and backup:
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        backup_path = output_path + f".bak.{ts}"
        shutil.move(output_path, backup_path)
        print(f"Backed up existing output to {backup_path}")
    shutil.copy2(template_path, output_path)
    print(f"Initialized fresh output from template -> {os.path.abspath(output_path)}")


def _is_nonempty(x) -> bool:
    return pd.notna(x) and str(x).strip().lower() not in {'', 'nan', 'none'}

def _choose_id(row) -> str:
    serial = row.Serial_Number if hasattr(row, 'Serial_Number') else None
    tag = row.Tag_Number if hasattr(row, 'Tag_Number') else None
    if _is_nonempty(serial):
        return str(serial).strip()
    if _is_nonempty(tag):
        return str(tag).strip()
    return ''

# %%
def load_files(equipment_path, hierarchy_path, loadsheet_path):
    equip_df = pd.read_excel(
        equipment_path,
        usecols=["Serial Number", "Tag Number", "Mfg Desc", "Product Model", "Description", "Description 2", "Description 3"]
        )
    hierarchy_df = pd.read_excel(
        hierarchy_path,
        usecols=["level_4", "level_4_1", "level_5", "level_5_1", "level_6", "level_6_1", "level_7", "level_8", "subclass"]
        )

    return equip_df, hierarchy_df

# %%
def preprocess_dataframes(hierarchy_df, equip_df):
    # Pre processing of some of the dataframes

    # hierarchy_df
    # Create normalized tags for comparison later
    # Normalize Columns
    for col in ['level_6_1', 'level_7', 'level_8']:
        if col in hierarchy_df.columns:
            hierarchy_df[f'{col}_normalized'] = (hierarchy_df[col].astype(str).str.replace('-', '', regex=False).str.strip())
        else:
            logging.warning(f"Column '{col}' not found in the Excel file.")

    # equip_df
    # Replace any spaces with "_" in the column
    # This is important for the Named tuples we use later
    equip_df.columns = equip_df.columns.str.replace(" ", "_")

# %%
def static_variables():
    #  Defining some of the output columns and rows for the simpleload.xlsx file
    # We will define maps here for LLM outputs to code if we need to, but will likely just train LLM to output the right code.
    
    config = _load_config()
    row_start = config.get("row_start", 3)
    FLOC_sheet = config.get("FLOC_sheet")
    equip_sheet = config.get("equip_sheet")
    return row_start, FLOC_sheet, equip_sheet

def _build_hierarchy_desc_map():
    data = _load_config()
    target_keys = ['L4_codes', 'L4_1_codes', 'L5_codes', 'L5_1_codes']
    m = {}
    for key in target_keys:
        for desc, code in data.get(key, {}).items():
            m[str(code).replace('-', '').strip()] = desc
    return m

def build_hierarchy_index(hierarchy_df):
    # make sure normalized columns exist (preprocess already does this)
    index = {}
    cols = ['level_6_1_normalized', 'level_7_normalized', 'level_8_normalized']
    for _, row in hierarchy_df.iterrows():
        for c in cols:
            key = row.get(c)
            if pd.notna(key):
                index[str(key)] = row

    return index

# %%
def get_hierarchy_chain(start_id, hierarchy_index, hierarchy_desc_map, equip_row: pd.Series) -> List[Dict[str, Any]]:
    if not start_id:
        return []
    
    normalized_id = start_id.replace('-', '').strip()
    match_row = hierarchy_index.get(normalized_id)
    if match_row is None:
        logging.warning(f"No match found in hierarchy for ID '{normalized_id}'")
        return []
    
    equipment_levels = {'level_6_1', 'level_7', 'level_8'}
    hierarchy_columns = ['level_4', 'level_4_1', 'level_5', 'level_5_1', 'level_6', 'level_6_1']

    chain = []
    for i in reversed(range(len(hierarchy_columns))):
        col = hierarchy_columns[i]
        raw_value = match_row[col]
        if pd.isna(raw_value):
            continue

        parts = [str(match_row[c]) for c in hierarchy_columns[:i+1] if pd.notna(match_row[c])]
        parent = [str(match_row[c]) for c in hierarchy_columns[:i] if pd.notna(match_row[c])]
        entry = {'ID': '-'.join(parts), 'Superior FLOC': '-'.join(parent) if parent else None}
        
        if col in equipment_levels:
            entry.update({
                'Subclass': match_row.get('subclass', ''),
                'Make': str(equip_row.get('Mfg_Desc', '')).strip(),
                'Model': str(equip_row.get('Product_Model', '')).strip(),
                'Description': "; ".join(
                    str(equip_row[c]).strip()
                    for c in ['Description', 'Description_2', 'Description_3']
                    if pd.notna(equip_row.get(c))
                )
            })
        else:
            desc_key = str(raw_value).replace('-', '').strip()
            desc = hierarchy_desc_map.get(desc_key)
            if desc:
                entry['Description'] = desc

        chain.append(entry)

    return chain

# %%
def write_chain_to_output(ws, chain, row_start, FLOC_sheet, current_row_offset=0):
    col_id = FLOC_sheet["ID (Blank if Equipment)"]
    col_parent = FLOC_sheet["Superior FLOC (Parent)"]
    col_class = FLOC_sheet["Class (DCAM Subclass)"]
    col_desc = FLOC_sheet["Description"]
    col_make = FLOC_sheet["Make"]
    col_model = FLOC_sheet["Model"]

    for i, entry in enumerate(chain):
        r = row_start + current_row_offset + i
        ws.cell(row=r, column=col_id, value=entry.get('ID'))
        ws.cell(row=r, column=col_parent, value=entry.get('Superior FLOC'))
        ws.cell(row=r, column=col_class, value=entry.get('Subclass', ''))
        if 'Description' in entry: ws.cell(row=r, column=col_desc, value=entry['Description'])
        if 'Make' in entry: ws.cell(row=r, column=col_make, value=entry['Make'])
        if 'Model' in entry: ws.cell(row=r, column=col_model, value=entry['Model'])
    
    return current_row_offset + len(chain), ws

# %%
def extract_from_sheets(equip_df, hierarchy_df, row_start, loadsheet_path, FLOC_sheet, equipment_start_index=0, save_every=50):

    wb = load_workbook(loadsheet_path)
    ws = wb[FLOC_sheet["sheet_name"]] # This will need to be put in the for loop, when we need to move between multiple sheets - like if we need to fill out the FLOCEquip sheet 
    
    # Build once (fast lookups)
    hierarchy_index = build_hierarchy_index(hierarchy_df)
    hierarchy_desc_map = _build_hierarchy_desc_map()
    
    
    current_row_offset = 0
    # print(equip_df) # debugging
    # input() # debugging
    try:
        for i, row in enumerate(tqdm(equip_df.itertuples(index=True), total=len(equip_df), desc='Processing Equipment'), start=1):
            id_val = _choose_id(row)
            chain = get_hierarchy_chain(id_val, hierarchy_index, hierarchy_desc_map, pd.Series(row._asdict()))
            current_row_offset, ws = write_chain_to_output(ws, chain, row_start, FLOC_sheet, current_row_offset)
            
            #periodic checkpoints
            if i % save_every == 0:
                wb.save(loadsheet_path)
                save_progress(loadsheet_path, equipment_start_index + i, row_start, current_row_offset)

        wb.save(loadsheet_path)
        save_progress(loadsheet_path, equipment_start_index + len(equip_df), row_start, current_row_offset)

    finally:
        # last-ditch save even if interrupted
        try:
            wb.save(loadsheet_path)
        except Exception as e:
            logging.warning(f"Final save failed: {e}")
        wb.close()

def excel_row_to_df_index_equip(excel_row):
    # Calibration to help start from the endpoint. The input is the excel row number where the next equipment is that should go into the output should be.
    return excel_row - 2

# %%
def main():
    # Define the files that we are working with
    log_output_path = 'equipment_match.log'
    equipment_path = 'data/Current JDE Equipment Table_NoFilter.xlsx' #'./data/Current JDE Equipment Table 6-2-25.xlsx'
    hierarchy_path = './data/hierarchy_output.xlsx'
    template_path = './data/simpleload.xlsx'
    loadsheet_path = './data/simpleload_output.xlsx'

    if not os.path.isfile(template_path):
        raise FileNotFoundError(f"Template file not found: {os.path.abspath(template_path)}")
    
    # Logging
    config_logging(log_output_path)

    progress = load_progress(loadsheet_path)
    if progress:
        print(f"Resuming from progress: {progress}")
        equipment_start_index = progress.get("equipment_start_index", 0)
        pending_current_row = progress.get("current_row", None)
        starting_fresh = False
    else:
        print("No progress file found - starting from scratch")
        reset_output_from_template(template_path, loadsheet_path, backup=True)
        equipment_start_index = 0
        pending_current_row = None
        starting_fresh = True

    # Load files into Dataframes and dictionaries for use using pandas
    equip_df, hierarchy_df = load_files(equipment_path, hierarchy_path, loadsheet_path)

    # Preprocess dataframes - create normailized equipment columns with normalized tags for the hierarchy df and Replace any spaces with "_" in the column headers for the equipment df
    preprocess_dataframes(hierarchy_df, equip_df)

    # Define the start of rows in the FLOC_sheet, Define the columns in the FLOC and equipment sheet
    # equip_sheet is the excel sheet that hast the equipment from JDE listed on it, compes from equipment_path
    # FLOC_sheet is one of the sheets 'FLOC Only' sheet from the loadsheet and comes from loadsheet path. This is our output.
    row_start, FLOC_sheet, equip_sheet = static_variables()

    output_start_row = pending_current_row if pending_current_row is not None else row_start

    # run the sheet
    extract_from_sheets(
        equip_df.iloc[equipment_start_index:].copy(),
        hierarchy_df,
        output_start_row,
        loadsheet_path,
        FLOC_sheet,
        equipment_start_index = equipment_start_index,
        save_every=50
    )
    
if __name__ == "__main__":
    print("Chromes burning...")
    main()
    print("Ran hot, cooled clean")

