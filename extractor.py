import os
import openpyxl
import datetime
from openpyxl.worksheet.worksheet import Worksheet
from openpyxl.utils import get_column_letter, column_index_from_string
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
from functools import lru_cache
import time
from typing import Dict, List, Tuple, Optional, Any
import logging
import argparse # Import argparse

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuration ---
DATA_FOLDER = "Raw Data"
OUTPUT_FOLDER = "Outputs"
MAX_WORKERS = max(1, multiprocessing.cpu_count() - 1)

# Sheet names for different report versions
SHEET_1_NAME_V1 = "TEMPLATE INSPEKSI"
SHEET_1_NAME_V1_ALT = "TEMPALTE INSPEKSI"
SHEET_2_NAME_V1 = "Visual"

SHEET_1_NAME_V2 = "UT Data"
SHEET_2_NAME_V2 = "General Visual"

# Search range for the "REMARKS" cell in both sheets
SEARCH_REMARKS_END_COL = 'AD'
SEARCH_REMARKS_END_ROW = 25

# Global cache for column lookups
_column_cache = {}


# --- Performance Optimized Helper Functions ---

def get_column_values_batch(sheet: Worksheet, col_idx: int, start_row: int, end_row: int) -> List[Any]:
    """Reads all cell values in a column range at once for better performance."""
    if not col_idx or start_row > end_row:
        return []
    # Use a list comprehension for a more compact and often faster implementation
    return [row[0] for row in
            sheet.iter_rows(min_row=start_row, max_row=end_row, min_col=col_idx, max_col=col_idx, values_only=True)]


def get_area_values_batch(sheet: Worksheet, area: Tuple[int, int, int, int]) -> Dict[Tuple[int, int], Any]:
    """Read all cell values in an area at once for better performance."""
    if not area:
        return {}

    start_row, end_row, start_col, end_col = area
    values = {}

    # Read entire range at once using iter_rows
    for row_idx, row in enumerate(sheet.iter_rows(min_row=start_row, max_row=end_row,
                                                  min_col=start_col, max_col=end_col, values_only=True)):
        for col_idx, value in enumerate(row):
            if value is not None:
                values[(start_row + row_idx, start_col + col_idx)] = value

    return values


def search_for_value_optimized(sheet: Worksheet, area: Tuple[int, int, int, int],
                               keywords: List[str], offset: int = 0, row_offset: int = 0) -> str:
    """Optimized search with early termination and batch reading."""
    if not area:
        return "Not Found"

    # Get all values in the area at once
    area_values = get_area_values_batch(sheet, area)

    # Pre-compile keywords for faster comparison
    keywords_lower = [k.lower() for k in keywords]

    start_row, end_row, start_col, end_col = area

    for (row, col), value in area_values.items():
        if value and isinstance(value, str):
            value_lower = value.lower()
            for keyword in keywords_lower:
                if keyword in value_lower:
                    # Found match - get the offset value
                    target_row = row + row_offset
                    target_col = col + offset

                    # Check if target is within bounds
                    if target_row <= sheet.max_row and target_col <= sheet.max_column:
                        cell_value = sheet.cell(row=target_row, column=target_col).value

                        # Extract value from tuple if needed
                        if isinstance(cell_value, tuple) and len(cell_value) == 1:
                            return cell_value[0]
                        return cell_value

    return "Not Found"


def find_value_in_cell_optimized(sheet: Worksheet, area: Tuple[int, int, int, int], keyword: str) -> str:
    """Optimized version of find_value_in_cell."""
    if not area:
        return "Not Found"

    area_values = get_area_values_batch(sheet, area)
    keyword_lower = keyword.lower()

    for (row, col), value in area_values.items():
        if value and isinstance(value, str) and keyword_lower in value.lower():
            return value

    return "Not Found"


@lru_cache(maxsize=128)
def find_column_by_keyword_cached(sheet_title: str, header_area: Tuple[int, int, int, int], keyword: str) -> Optional[
    int]:
    """Cached version of column finder to avoid repeated searches."""
    # Note: This is a simplified cache key - in real implementation,
    # you'd need to pass the sheet object differently
    return None


def find_column_by_keyword_optimized(sheet: Worksheet, header_area: Tuple[int, int, int, int], keyword: str) -> \
Optional[int]:
    """Optimized column finder with batch reading."""
    if not header_area:
        return None

    # Create cache key
    cache_key = (sheet.title, header_area, keyword.lower())
    if cache_key in _column_cache:
        return _column_cache[cache_key]

    area_values = get_area_values_batch(sheet, header_area)
    keyword_lower = keyword.lower()

    for (row, col), value in area_values.items():
        if value and isinstance(value, str) and keyword_lower in value.lower():
            _column_cache[cache_key] = col
            return col

    logger.warning(f"Could not find column for keyword '{keyword}' in sheet '{sheet.title}'")
    _column_cache[cache_key] = None
    return None


def find_check_mark_in_column_optimized(sheet: Worksheet, header_area: Tuple[int, int, int, int],
                                        data_area: Tuple[int, int, int, int], keyword: str) -> str:
    """Optimized check mark finder that supports normal check symbol and Wingdings 2 'P'."""
    if not header_area or not data_area:
        return "Not Found"

    target_col = find_column_by_keyword_optimized(sheet, header_area, keyword)
    if not target_col:
        return "Not Found"

    data_start_row, data_end_row, _, _ = data_area

    # Characters that represent a check mark
    check_symbols = {"√", "P"}

    # Read entire column at once
    column_values = get_column_values_batch(sheet, target_col, data_start_row, data_end_row)

    for cell_val in column_values:
        if cell_val and isinstance(cell_val, str) and cell_val.strip() in check_symbols:
            return "√"  # Always output a normal check mark symbol

    return "Not Found"


def extract_unique_values_from_col_optimized(sheet: Worksheet, data_area: Tuple[int, int, int, int],
                                             col_idx: int) -> str:
    """Optimized unique value extraction."""
    if not data_area or not col_idx:
        return "Not Found"

    data_start_row, data_end_row, _, _ = data_area
    unique_values = set()

    # Read entire column at once
    column_values = get_column_values_batch(sheet, col_idx, data_start_row, data_end_row)

    for cell_val in column_values:
        if cell_val and str(cell_val).strip():
            unique_values.add(str(cell_val).strip())

    return ", ".join(sorted(list(unique_values))) if unique_values else "Not Found"


def extract_unique_values_from_col_or_notfound(sheet: Worksheet, data_area: Tuple[int, int, int, int],
                                               col_idx: int) -> str:
    """
    Extract unique non-empty values from a column or return 'Not Found' if none.
    """
    if not data_area or not col_idx:
        return "Not Found"

    data_start_row, data_end_row, _, _ = data_area
    unique_values = set()

    column_values = get_column_values_batch(sheet, col_idx, data_start_row, data_end_row)

    for cell_val in column_values:
        if cell_val and str(cell_val).strip():
            unique_values.add(str(cell_val).strip())
    return ", ".join(sorted(list(unique_values))) if unique_values else "Not Found"


# --- Optimized Area Finding Functions ---

def find_area_by_remarks_optimized(sheet: Worksheet) -> Optional[Tuple[int, int, int, int]]:
    """Optimized remarks finder with batch reading."""
    start_row, start_col = 2, 2
    try:
        search_end_col_idx = column_index_from_string(SEARCH_REMARKS_END_COL)
    except Exception:
        logger.error(f"Invalid column '{SEARCH_REMARKS_END_COL}' in config.")
        return None

    search_area = (start_row, SEARCH_REMARKS_END_ROW, start_col, search_end_col_idx)
    area_values = get_area_values_batch(sheet, search_area)

    for (row, col), value in area_values.items():
        if value and isinstance(value, str) and "REMARKS".lower() in value.lower():
            return start_row, row, start_col, col

    logger.error(f"Could not find 'REMARKS' cell in range B2:{SEARCH_REMARKS_END_COL}{SEARCH_REMARKS_END_ROW}")
    return None


def find_data_area_sheet1_optimized(sheet: Worksheet, end_col_from_first_area: int, total_joint: Any) -> Optional[
    Tuple[int, int, int, int]]:
    """Modified data area finder for Sheet 1 - uses 'Approval Path' to find end row."""

    # Read column B once to find start and end rows
    col_b_values = get_column_values_batch(sheet, 2, 1, sheet.max_row)

    # Find start row with value '1' in Column B
    start_row = None
    # Search in first 100 rows, which is a reasonable limit
    for i, cell_val in enumerate(col_b_values[:min(len(col_b_values), 99)]):
        if cell_val == 1:
            start_row = i + 1  # 1-based index
            break

    if not start_row:
        logger.error("Could not find start row with value '1' in Column B.")
        return None

    # NEW LOGIC: Scan until 'Approval Path' is found
    end_row = None
    # Start search from where start_row was found
    for i, cell_val in enumerate(col_b_values[start_row - 1:]):
        if isinstance(cell_val, str) and cell_val.strip() == "Approval Path":
            end_row = start_row + i  # 1-based index
            break

    if not end_row:
        logger.error("Could not find 'Approval Path' in Column B.")
        return None

    return start_row, end_row, 2, end_col_from_first_area


def find_total_joint_from_data_area(sheet: Worksheet, start_row: int, end_row: int) -> Any:
    """Find the biggest number in Column B within the data area range."""
    max_number = None

    col_b_values = get_column_values_batch(sheet, 2, start_row, end_row)

    for cell_val in col_b_values:
        if isinstance(cell_val, (int, float)):
            if max_number is None or cell_val > max_number:
                max_number = cell_val

    return max_number if max_number is not None else "Not Found"


def find_areas_visual_optimized(sheet: Worksheet, total_joint: Any, report_type: str) -> Optional[
    Tuple[Tuple[int, int, int, int], Tuple[int, int, int, int]]]:
    """Optimized visual area finder. Special handling for v2 General Visual sheet (no Remarks column)."""
    try:
        if report_type == "v2" and sheet.title == SHEET_2_NAME_V2:
            # Fixed column AA as end limit
            remarks_col_idx = column_index_from_string("AA")
            logger.info("Using fixed end column AA for v2 General Visual (no Remarks column)")

            # Determine start column for data (v2 → column B)
            start_col_data = 2

            # Find data start row
            data_start_row = None
            col_b_values = get_column_values_batch(sheet, start_col_data, 1, min(sheet.max_row, 100))
            for i, cell_val in enumerate(col_b_values):
                if cell_val == 1:
                    data_start_row = i + 1
                    break
            if not data_start_row:
                logger.error(f"Could not find data start row with value '1' in Column {get_column_letter(start_col_data)}.")
                return None

            # Ensure total_joint is valid
            try:
                total_joint_num = int(total_joint)
            except (ValueError, TypeError):
                logger.error(f"'Total Joint' value ('{total_joint}') is not valid.")
                return None

            # Find data end row
            data_end_row = None
            col_b_all_values = get_column_values_batch(sheet, start_col_data, data_start_row, sheet.max_row)
            for i, cell_val in enumerate(col_b_all_values):
                if isinstance(cell_val, (int, float)) and int(cell_val) == total_joint_num:
                    next_cell_val = col_b_all_values[i + 1] if i + 1 < len(col_b_all_values) else None
                    if next_cell_val is None or not isinstance(next_cell_val, (int, float)) or int(
                            next_cell_val) != total_joint_num:
                        data_end_row = data_start_row + i
                        break
            if not data_end_row:
                logger.error(f"Could not find data end row matching 'Total Joint' {total_joint_num}.")
                return None

            # Build header and data areas
            header_area = (2, data_start_row - 1, 1, remarks_col_idx)
            data_area = (data_start_row, data_end_row, start_col_data, remarks_col_idx)
            return header_area, data_area

        else:
            # Original v1 logic
            search_end_col_idx = column_index_from_string(SEARCH_REMARKS_END_COL)
            search_area = (2, SEARCH_REMARKS_END_ROW, 2, search_end_col_idx)
            area_values = get_area_values_batch(sheet, search_area)

            remarks_cell_row, remarks_cell_col = None, None
            for (row, col), value in area_values.items():
                if value and isinstance(value, str) and "REMARKS".lower() in value.lower():
                    remarks_cell_row, remarks_cell_col = row, col
                    break

            if not remarks_cell_row:
                logger.error("Could not find 'REMARKS' cell in visual sheet.")
                return None

            start_col_data = 1 if report_type == "v1" else 2
            data_start_row = None
            col_start_values = get_column_values_batch(sheet, start_col_data, 1, min(sheet.max_row, 100))
            for i, cell_val in enumerate(col_start_values):
                if cell_val == 1:
                    data_start_row = i + 1
                    break
            if not data_start_row:
                logger.error(f"Could not find data start row with value '1' in Column {get_column_letter(start_col_data)}.")
                return None

            try:
                total_joint_num = int(total_joint)
            except (ValueError, TypeError):
                logger.error(f"'Total Joint' value ('{total_joint}') is not valid.")
                return None

            data_end_row = None
            col_start_all_values = get_column_values_batch(sheet, start_col_data, data_start_row, sheet.max_row)
            for i, cell_val in enumerate(col_start_all_values):
                if isinstance(cell_val, (int, float)) and int(cell_val) == total_joint_num:
                    next_cell_val = col_start_all_values[i + 1] if i + 1 < len(col_start_all_values) else None
                    if next_cell_val is None or not isinstance(next_cell_val, (int, float)) or int(
                            next_cell_val) != total_joint_num:
                        data_end_row = data_start_row + i
                        break
            if not data_end_row:
                logger.error(f"Could not find data end row matching 'Total Joint' {total_joint_num}.")
                return None

            data_area = (data_start_row, data_end_row, start_col_data, remarks_cell_col)
            header_area = (remarks_cell_row, data_start_row - 1, 1, remarks_cell_col)
            if header_area[0] >= header_area[1]:
                logger.warning("Header area has invalid dimensions.")
                return None
            return header_area, data_area

    except Exception as e:
        logger.error(f"Error in find_areas_visual_optimized: {e}")
        return None


def determine_document_version(workbook, sheet1_name=None) -> str:
    """
    Determine the document version based on sheet names and structure.

    Args:
        workbook: The openpyxl workbook object
        sheet1_name: Optional - the name of sheet1 if already determined

    Returns:
        str: "v1", "v2", or "unknown"
    """
    try:
        sheet_names = workbook.sheetnames

        # Check for version 1 indicators
        v1_indicators = [
            SHEET_1_NAME_V1 in sheet_names,
            SHEET_1_NAME_V1_ALT in sheet_names,
            SHEET_2_NAME_V1 in sheet_names
        ]

        # Check for version 2 indicators
        v2_indicators = [
            SHEET_1_NAME_V2 in sheet_names,
            SHEET_2_NAME_V2 in sheet_names
        ]

        if any(v1_indicators):
            return "v1"
        elif any(v2_indicators):
            return "v2"
        else:
            return "unknown"

    except Exception as e:
        logger.warning(f"Error determining document version: {e}")
        return "unknown"


# --- Optimized Extraction Functions ---

def extract_min_thickness_and_joint_optimized(sheet: Worksheet, data_area: Tuple[int, int, int, int],
                                              header_area: Tuple[int, int, int, int]) -> Tuple[Any, str]:
    """Optimized minimum thickness extraction, excluding zero values."""
    if not header_area:
        return "Not Found", "Not Found"

    thickness_col = find_column_by_keyword_optimized(sheet, header_area, "MINIMUM THICKNESS")
    if not thickness_col:
        return "Not Found", "Not Found"

    start_row, end_row, _, _ = data_area

    # Read required columns in batch
    thickness_vals = get_column_values_batch(sheet, thickness_col, start_row, end_row)
    col_b_vals = get_column_values_batch(sheet, 2, start_row, end_row)
    col_c_vals = get_column_values_batch(sheet, 3, start_row, end_row)

    min_thickness_data = []
    for i, cell_val in enumerate(thickness_vals):
        if isinstance(cell_val, (int, float)) and cell_val > 0:
            min_thickness_data.append({'value': cell_val, 'index': i})

    if not min_thickness_data:
        return "Not Found", "Not Found"

    min_item = min(min_thickness_data, key=lambda x: x['value'])
    row_idx = min_item['index']

    val_b = col_b_vals[row_idx]
    val_c = col_c_vals[row_idx]
    joint = f"{val_b or ''}{val_c or ''}"

    return min_item['value'], joint


def extract_max_thickness_and_joint_optimized(sheet: Worksheet, data_area: Tuple[int, int, int, int],
                                              header_area: Tuple[int, int, int, int]) -> Tuple[Any, str]:
    """Optimized minimum thickness extraction."""
    if not header_area:
        return "Not Found", "Not Found"

    thickness_col = find_column_by_keyword_optimized(sheet, header_area, "MAXIMUM THICKNESS")
    if not thickness_col:
        return "Not Found", "Not Found"

    start_row, end_row, _, _ = data_area

    # Read required columns in batch
    thickness_vals = get_column_values_batch(sheet, thickness_col, start_row, end_row)
    col_b_vals = get_column_values_batch(sheet, 2, start_row, end_row)
    col_c_vals = get_column_values_batch(sheet, 3, start_row, end_row)

    thickness_data = []
    for i, cell_val in enumerate(thickness_vals):
        if isinstance(cell_val, (int, float)):
            thickness_data.append({'value': cell_val, 'index': i})

    if not thickness_data:
        return "Not Found", "Not Found"

    max_item = max(thickness_data, key=lambda x: x['value'])
    row_idx = max_item['index']

    val_b = col_b_vals[row_idx]
    val_c = col_c_vals[row_idx]
    joint = f"{val_b or ''}{val_c or ''}"

    return max_item['value'], joint


def extract_max_thickness_pipe_joint_optimized(sheet: Worksheet, data_area: Tuple[int, int, int, int],
                                               header_area: Tuple[int, int, int, int]) -> Any:
    """
    Extract maximum thickness only for joints where JOINT TYPES is 'PIPE'.
    """
    if not header_area:
        return "="

    thickness_col = find_column_by_keyword_optimized(sheet, header_area, "MAXIMUM THICKNESS")
    joint_type_col = find_column_by_keyword_optimized(sheet, header_area, "JOINT TYPES")
    if not thickness_col or not joint_type_col:
        return "="

    start_row, end_row, _, _ = data_area

    thickness_vals = get_column_values_batch(sheet, thickness_col, start_row, end_row)
    joint_type_vals = get_column_values_batch(sheet, joint_type_col, start_row, end_row)

    max_thickness = None
    for i, joint_type_val in enumerate(joint_type_vals):
        thickness_val = thickness_vals[i]

        if isinstance(joint_type_val, str) and joint_type_val.strip().upper() == "PIPE":
            if isinstance(thickness_val, (int, float)):
                if max_thickness is None or thickness_val > max_thickness:
                    max_thickness = thickness_val
    return max_thickness if max_thickness is not None else "="


def extract_from_sheet1_optimized(sheet: Worksheet, report_type: str) -> Dict[str, Any]:
    """Optimized extraction function for Sheet 1."""
    logger.info(f"Processing sheet: {sheet.title} (Type: {report_type})")

    header_area = find_area_by_remarks_optimized(sheet)
    if not header_area:
        return {}

    start_r, end_r, start_c, end_c = header_area
    logger.info(f"Header Area: {get_column_letter(start_c)}{start_r}:{get_column_letter(end_c)}{end_r}")

    # Extract basic data
    data = {
        "Inspection Date Finish": sheet['E4'].value,
        "Length Of Inspection (m)": search_for_value_optimized(sheet, header_area, ["LENGTH"], offset=2),
        "Pipe Material": search_for_value_optimized(sheet, header_area, ["PIPE MATERIAL"], offset=2),
        "Operating Pressure (psi)": search_for_value_optimized(sheet, header_area, ["PRESSURE"], offset=2),
        "Operating Temperature (F)": search_for_value_optimized(sheet, header_area, ["TEMPERATURE"], offset=2),
        "Service Fluid": search_for_value_optimized(sheet, header_area, ["FLUID"], offset=2),
        "Line ID": search_for_value_optimized(sheet, header_area, ["LINE ID"], offset=2),
        "NPS (in)": search_for_value_optimized(sheet, header_area, ["NOMINAL PIPE SIZE"], offset=2),
        "Estimation Year Buil": search_for_value_optimized(sheet, header_area, ["YEAR"], offset=2),
        "Pipe Segment": search_for_value_optimized(sheet, header_area, ["PIPE SEGMENT"], offset=2)
    }

    if report_type == "v1":
        data["Length Of Inspection (m)"] = search_for_value_optimized(sheet, header_area, ["LENGTH"], offset=2)
        data["Nominal Thickness (in)"] = search_for_value_optimized(sheet, header_area, ["NOMINAL THICKNESS"],
                                                                    offset=2, row_offset=1)
        data["Flange Rating"] = find_value_in_cell_optimized(sheet, header_area, "ANSI")
    elif report_type == "v2":
        data["Length Of Inspection (m)"] = search_for_value_optimized(sheet, header_area, ["LENGTH"], offset=1)
        data["Nominal Thickness (mm)"] = search_for_value_optimized(sheet, header_area, ["NOMINAL THICKNESS"],
                                                                    offset=2)
        data["Flange Rating"] = search_for_value_optimized(sheet, header_area, ["ANSI"], row_offset=1)

    # Process data area
    data_area = find_data_area_sheet1_optimized(sheet, end_c, None)  # Pass None since we don't need it

    if data_area:
        logger.info(
            f"Data Area: {get_column_letter(data_area[2])}{data_area[0]}:{get_column_letter(data_area[3])}{data_area[1]}")

        start_row, end_row, start_col, end_col = data_area
        total_joint_calculated = find_total_joint_from_data_area(sheet, start_row, end_row)
        data["Total Joint"] = total_joint_calculated

        header_area_2 = (end_r, data_area[0] - 1, 2, end_c)

        min_thick, min_joint_loc = extract_min_thickness_and_joint_optimized(sheet, data_area, header_area_2)
        data["Minimum Thickness (mm)"] = min_thick
        data["Joint of Minimum Thickness"] = min_joint_loc

        max_thick, max_joint_loc = extract_max_thickness_and_joint_optimized(sheet, data_area, header_area_2)
        data["Maximum Thickness (mm)"] = max_thick
        data["Joint of Maximum Thickness"] = max_joint_loc

        max_thick_pipe = extract_max_thickness_pipe_joint_optimized(sheet, data_area, header_area_2)
        data["Max Thickness Pipe Joint"] = max_thick_pipe

        data["Remarks"] = extract_unique_values_from_col_optimized(sheet, data_area, end_c)

    return data


def calculate_inspection_rate_ratio(sheet, data_area, nps_size):
    """
    Calculate inspection rate ratio for each joint in Sheet 1.

    Args:
        sheet: Sheet 1 worksheet object
        data_area: Tuple (start_row, end_row, start_col, end_col) for Sheet 1 data
        nps_size: NPS (in) size value to determine the divisor

    Returns:
        Dict mapping joint numbers to their averaged inspection rate ratios
    """
    logger.info("Calculating inspection rate ratios...")

    # Determine divisor based on NPS size
    try:
        nps_value = float(nps_size) if nps_size and nps_size != "Not Found" else 0
        if nps_value < 4:
            divisor = 2
        elif 4 <= nps_value <= 12:
            divisor = 4
        else:  # nps_value > 12
            divisor = 6

        logger.info(f"NPS size: {nps_value}, using divisor: {divisor}")
    except (ValueError, TypeError):
        logger.warning(f"Invalid NPS size '{nps_size}', using default divisor of 4")
        divisor = 4

    # Define the inspection columns (D to O)
    inspection_start_col = 4  # Column D
    inspection_end_col = 15  # Column O
    joint_col = 2  # Column B (joint identifier)

    # Dictionary to store all ratios for each joint
    joint_ratios = {}

    start_row, end_row, _, _ = data_area

    # Read the entire data block at once
    data_block = list(sheet.iter_rows(min_row=start_row, max_row=end_row,
                                      min_col=joint_col, max_col=inspection_end_col,
                                      values_only=True))

    # Column indices relative to the start of the data_block
    joint_col_idx = 0  # joint_col (2) - min_col (2)
    inspection_start_idx = inspection_start_col - joint_col  # 4 - 2 = 2
    inspection_end_idx = inspection_end_col - joint_col  # 15 - 2 = 13

    # Process each row from the in-memory data block
    for row_data in data_block:
        joint_val = row_data[joint_col_idx]

        if joint_val is not None:
            try:
                joint_num = int(joint_val)

                # Slice the row data to get only inspection columns
                inspection_values = row_data[inspection_start_idx: inspection_end_idx + 1]

                numeric_count = 0
                for cell_val in inspection_values:
                    if cell_val is not None and cell_val != "" and isinstance(cell_val, (int, float)):
                        numeric_count += 1

                ratio = numeric_count / divisor

                if joint_num not in joint_ratios:
                    joint_ratios[joint_num] = []
                joint_ratios[joint_num].append(ratio)

                logger.debug(f"Joint {joint_num}: {numeric_count} numeric values, ratio = {ratio:.3f}")

            except (ValueError, TypeError):
                logger.warning(f"Invalid joint value: {joint_val}")
                continue

    # Calculate average ratio for each joint
    joint_avg_ratios = {}
    for joint_num, ratios in joint_ratios.items():
        if ratios:
            avg_ratio = sum(ratios) / len(ratios)
            joint_avg_ratios[joint_num] = round(avg_ratio, 3)
            logger.info(f"Joint {joint_num}: {len(ratios)} occurrences, average ratio = {avg_ratio:.3f}")

    return joint_avg_ratios


def write_minimum_thickness_to_sheet2(workbook, sheet1, sheet2, sheet1_data_area, sheet2_data_area,
                                      sheet1_header_area,
                                      report_type):
    """
    Write minimum thickness values from Sheet 1 to Sheet 2.

    Args:
        workbook: The openpyxl workbook object
        sheet1: Sheet 1 worksheet object
        sheet2: Sheet 2 worksheet object
        sheet1_data_area: Tuple (start_row, end_row, start_col, end_col) for Sheet 1 data
        sheet2_data_area: Tuple (start_row, end_row, start_col, end_col) for Sheet 2 data
        sheet1_header_area: Tuple for Sheet 1 header area
        report_type: "v1" or "v2"
    """
    logger.info("Writing minimum thickness values to Sheet 2...")

    sheet1_data_header_area = (sheet1_header_area[1], sheet1_data_area[0] - 1, 2, sheet1_header_area[3])
    # Find thickness column in Sheet 1
    thickness_col = find_column_by_keyword_optimized(sheet1, sheet1_data_header_area, "MINIMUM THICKNESS")
    if not thickness_col:
        logger.error("Could not find thickness column in Sheet 1")
        logger.error(f"Searched in header area: {sheet1_data_header_area}")
        return False

    # Find joint column in Sheet 1 (usually column B)
    sheet1_joint_col = 2  # Column B

    # Collect thickness data by joint from Sheet 1 using batch read
    s1_start_row, s1_end_row, _, _ = sheet1_data_area
    joint_vals = get_column_values_batch(sheet1, sheet1_joint_col, s1_start_row, s1_end_row)
    thickness_vals = get_column_values_batch(sheet1, thickness_col, s1_start_row, s1_end_row)

    joint_thickness_map = {}
    for i, joint_val in enumerate(joint_vals):
        thickness_val = thickness_vals[i]

        if joint_val is not None and thickness_val is not None:
            # Convert to appropriate types
            try:
                joint_num = int(joint_val)
                thickness_num = float(thickness_val)

                # Include all values (including 0) in the collection
                if joint_num not in joint_thickness_map:
                    joint_thickness_map[joint_num] = []
                joint_thickness_map[joint_num].append(thickness_num)
            except (ValueError, TypeError):
                continue

    # Calculate minimum thickness for each joint group
    joint_min_thickness = {}
    for joint_num, thickness_list in joint_thickness_map.items():
        if thickness_list:
            # First try to find minimum value > 0
            non_zero_values = [t for t in thickness_list if t > 0]
            if non_zero_values:
                joint_min_thickness[joint_num] = min(non_zero_values)
            else:
                # If all values are 0 or negative, return 0
                joint_min_thickness[joint_num] = 0
            logger.info(
                f"Joint {joint_num}: min thickness = {joint_min_thickness[joint_num]} (from values: {thickness_list})")

    # Determine joint column in Sheet 2
    sheet2_joint_col = 1 if report_type == "v1" else 2  # Column A for v1, Column B for v2
    s2_start_row, s2_end_row, _, _ = sheet2_data_area

    # Batch read joint column from sheet 2
    sheet2_joint_vals = get_column_values_batch(sheet2, sheet2_joint_col, s2_start_row, s2_end_row)

    # Determine the column to write thickness values (after data area end column)
    write_col = sheet2_data_area[3] + 1  # Next column after data area end

    # Write header for the new column
    header_row = sheet2_data_area[0] - 1  # Row above data area
    sheet2.cell(row=header_row, column=write_col, value="Min Thickness")

    # Write minimum thickness values to Sheet 2
    written_count = 0
    for i, joint_val in enumerate(sheet2_joint_vals):
        row = s2_start_row + i
        if joint_val is not None:
            try:
                joint_num = int(joint_val)
                if joint_num in joint_min_thickness:
                    sheet2.cell(row=row, column=write_col, value=joint_min_thickness[joint_num])
                    written_count += 1
                else:
                    # If no matching joint found, write "N/A" or leave empty
                    sheet2.cell(row=row, column=write_col, value="N/A")
            except (ValueError, TypeError):
                sheet2.cell(row=row, column=write_col, value="N/A")

    logger.info(f"Successfully wrote {written_count} minimum thickness values to Sheet 2")
    return True


def write_inspection_rate_ratio_to_sheet2(workbook, sheet1, sheet2, sheet1_data_area, sheet2_data_area,
                                          sheet1_header_area, report_type):
    """
    Calculate inspection rate ratios from Sheet 1 and write them to Sheet 2.
    Now retrieves NPS size from sheet1 data.
    """
    logger.info("Writing inspection rate ratios to Sheet 2...")

    # Get NPS size from sheet1 data
    sheet1_header_search_area = find_area_by_remarks_optimized(sheet1)
    if sheet1_header_search_area:
        nps_size = search_for_value_optimized(sheet1, sheet1_header_search_area, ["NOMINAL PIPE SIZE"], offset=2)
    else:
        nps_size = "Not Found"
        logger.warning("Could not find header area to get NPS size")

    # Calculate inspection rate ratios from Sheet 1 with NPS-based divisor
    joint_ratios = calculate_inspection_rate_ratio(sheet1, sheet1_data_area, nps_size)

    if not joint_ratios:
        logger.warning("No inspection rate ratios calculated")
        return False

    # Determine joint column in Sheet 2
    sheet2_joint_col = 1 if report_type == "v1" else 2  # Column A for v1, Column B for v2
    s2_start_row, s2_end_row, _, _ = sheet2_data_area

    # Batch read joint column from sheet 2
    sheet2_joint_vals = get_column_values_batch(sheet2, sheet2_joint_col, s2_start_row, s2_end_row)

    # Determine the column to write ratio values (after minimum thickness column)
    # Min thickness is at data_area_end + 1, so ratio goes at data_area_end + 2
    ratio_col = sheet2_data_area[3] + 2  # Two columns after data area end

    # Write header for the new column
    header_row = sheet2_data_area[0] - 1  # Row above data area
    sheet2.cell(row=header_row, column=ratio_col, value="Inspection Rate Ratio")

    # Write ratio values to Sheet 2
    written_count = 0
    for i, joint_val in enumerate(sheet2_joint_vals):
        row = s2_start_row + i
        if joint_val is not None:
            try:
                joint_num = int(joint_val)
                if joint_num in joint_ratios:
                    sheet2.cell(row=row, column=ratio_col, value=joint_ratios[joint_num])
                    written_count += 1
                else:
                    # If no matching joint found, write "N/A"
                    sheet2.cell(row=row, column=ratio_col, value="N/A")
            except (ValueError, TypeError):
                sheet2.cell(row=row, column=ratio_col, value="N/A")

    logger.info(f"Successfully wrote {written_count} inspection rate ratios to Sheet 2")
    return True


def extract_from_sheet2_optimized(sheet: Worksheet, total_joint: Any, report_type: str) -> Dict[str, Any]:
    """Optimized extraction function for Sheet 2."""
    logger.info(f"Processing sheet: {sheet.title} (Type: {report_type})")

    if not total_joint:
        logger.warning("Cannot process without 'Total Joint' value from Sheet 1.")
        return {}

    areas = find_areas_visual_optimized(sheet, total_joint, report_type)
    if not areas:
        return {}

    header_area, data_area = areas
    if report_type == "v2" and sheet.title == SHEET_2_NAME_V2:
        from openpyxl.utils import column_index_from_string
        remarks_col_idx = column_index_from_string("AA")
        header_area = (header_area[0], header_area[1], header_area[2], remarks_col_idx)
        data_area = (data_area[0], data_area[1], data_area[2], remarks_col_idx)
        logger.info("Overriding Remarks column to AA for v2 General Visual sheet")

    logger.info(
        f"Header Area: {get_column_letter(header_area[2])}{header_area[0]}:{get_column_letter(header_area[3])}{header_area[1]}")
    logger.info(
        f"Data Area: {get_column_letter(data_area[2])}{data_area[0]}:{get_column_letter(data_area[3])}{data_area[1]}")

    condition_keywords = [
        "Above Ground", "Lay Down", "Under Ground", "Sleeve",
        "Clamp", "Isolation"
    ]

    data = {}

    for keyword in condition_keywords:
        col_idx = find_column_by_keyword_optimized(sheet, header_area, keyword)
        unique_vals = extract_unique_values_from_col_or_notfound(sheet, data_area, col_idx)
        data[
            f"{keyword} Position" if 'Ground' in keyword or 'Lay Down' in keyword else f"{keyword} Condition"] = unique_vals

    # Handle painting condition
    painting_col_idx = find_column_by_keyword_optimized(sheet, header_area, "Painting")
    data["Painting Condition"] = extract_unique_values_from_col_optimized(sheet, data_area, painting_col_idx)

    # Handle remarks
    remarks_col_idx = header_area[3]
    data["Remarks Visual"] = extract_unique_values_from_col_optimized(sheet, data_area, remarks_col_idx)

    return data


def process_single_file_extraction_only(file_path: str) -> Dict[str, Any]:
    """
    Processes a single Excel file to extract data without modifying the original file.
    Handles unknown templates by recording them with minimal data.
    """
    filename = os.path.basename(file_path)
    logger.info(f"Extracting data from file: {filename}")

    try:
        # Load workbook in read-only mode for extraction only
        workbook = openpyxl.load_workbook(file_path, read_only=True, data_only=True)

        combined_data = {"File Name": filename}

        # Determine document version first
        document_version = determine_document_version(workbook)
        combined_data["Document Version"] = document_version

        # If the version is unknown, log it and return the basic data.
        # This will result in a row with mostly empty values in the output.
        if document_version == "unknown":
            logger.warning(f"'{filename}' has an unknown template. Recording with version only.")
            workbook.close()
            return combined_data

        report_type = document_version  # Use the determined version
        sheet1, sheet2 = None, None

        # Get sheets based on the determined version
        if report_type == "v1":
            sheet1 = workbook[SHEET_1_NAME_V1] if SHEET_1_NAME_V1 in workbook.sheetnames else workbook[
                SHEET_1_NAME_V1_ALT]
            if SHEET_2_NAME_V1 in workbook.sheetnames:
                sheet2 = workbook[SHEET_2_NAME_V1]
        elif report_type == "v2":
            sheet1 = workbook[SHEET_1_NAME_V2]
            if SHEET_2_NAME_V2 in workbook.sheetnames:
                sheet2 = workbook[SHEET_2_NAME_V2]

        if not sheet1:
            logger.warning(f"Could not find primary data sheet for {filename} (Version: {report_type}). Treating as unknown.")
            workbook.close()
            # Return basic data if the main sheet is missing
            return {"File Name": filename, "Document Version": document_version}

        # Process Sheet 1
        data_from_s1 = extract_from_sheet1_optimized(sheet1, report_type)
        combined_data.update(data_from_s1)

        # Process Sheet 2
        total_joint = combined_data.get("Total Joint")
        if sheet2 and total_joint:
            data_from_s2 = extract_from_sheet2_optimized(sheet2, total_joint, report_type)
            combined_data.update(data_from_s2)
        elif not sheet2:
            logger.warning(f"Sheet 2 not found for {filename}, skipping visual data extraction.")
        elif not total_joint:
            logger.warning(f"Total Joint not found in Sheet 1 for {filename}, skipping visual data extraction.")

        workbook.close()
        return combined_data

    except Exception as e:
        logger.error(f"Error processing {filename} for extraction only: {str(e)}")
        return {"File Name": filename, "Error": str(e)}


def process_single_file_with_calculations_and_updates(file_path: str) -> Dict[str, Any]:
    """
    Processes a single Excel file, performs calculations (min thickness, inspection rate ratio),
    and writes the results back to the original file. Skips files with unknown templates.
    """
    filename = os.path.basename(file_path)
    logger.info(f"Processing file for calculations and updates: {filename}")

    try:
        # Load workbook in write mode (not read_only)
        workbook = openpyxl.load_workbook(file_path, read_only=False, data_only=True)

        # Determine document version first
        document_version = determine_document_version(workbook)

        # If the version is unknown, we cannot process it for updates.
        if document_version == "unknown":
            logger.warning(f"'{filename}' has an unknown template. Skipping calculations and updates.")
            workbook.close()
            # Return an error so it's logged correctly in the calling function.
            return {"File Name": filename, "Error": "Unknown template; skipped update."}

        combined_data = {"File Name": filename}
        report_type = document_version
        sheet1, sheet2 = None, None

        # Determine report type and get sheets
        if report_type == "v1":
            sheet1 = workbook[SHEET_1_NAME_V1] if SHEET_1_NAME_V1 in workbook.sheetnames else workbook[
                SHEET_1_NAME_V1_ALT]
            if SHEET_2_NAME_V1 in workbook.sheetnames:
                sheet2 = workbook[SHEET_2_NAME_V1]
        elif report_type == "v2":
            sheet1 = workbook[SHEET_1_NAME_V2]
            if SHEET_2_NAME_V2 in workbook.sheetnames:
                sheet2 = workbook[SHEET_2_NAME_V2]

        if not sheet1:
            logger.warning(f"Could not find primary data sheet for {filename} (Version: {report_type}). Skipping calculations.")
            workbook.close()
            return {"File Name": filename, "Error": "Could not find primary data sheet"}

        # Extract data from Sheet 1 (needed for total_joint and data_area)
        sheet1_header_area = find_area_by_remarks_optimized(sheet1)
        if not sheet1_header_area:
            logger.error(f"Could not find header area in Sheet 1 for {filename}")
            workbook.close()
            return {"File Name": filename, "Error": "Could not find header area in Sheet 1"}

        data_from_s1 = extract_from_sheet1_optimized(sheet1, report_type)
        combined_data.update(data_from_s1)

        # Get Sheet 1 data area
        total_joint = combined_data.get("Total Joint")
        sheet1_data_area = find_data_area_sheet1_optimized(sheet1, sheet1_header_area[3], total_joint)

        # Perform calculations and write to Sheet 2
        if sheet2 and sheet1_data_area:
            # Get Sheet 2 areas
            sheet2_areas = find_areas_visual_optimized(sheet2, total_joint, report_type)
            if sheet2_areas:
                sheet2_header_area, sheet2_data_area = sheet2_areas

                # Write minimum thickness values to Sheet 2
                thickness_success = write_minimum_thickness_to_sheet2(
                    workbook, sheet1, sheet2, sheet1_data_area, sheet2_data_area, sheet1_header_area, report_type
                )

                # Write inspection rate ratios to Sheet 2
                ratio_success = write_inspection_rate_ratio_to_sheet2(
                    workbook, sheet1, sheet2, sheet1_data_area, sheet2_data_area, sheet1_header_area, report_type
                )

                if thickness_success or ratio_success:
                    # Save the modified workbook
                    workbook.save(file_path)
                    logger.info(f"Successfully updated {filename} with calculated values.")
                else:
                    logger.warning(f"No calculations were successfully written to {filename}.")

            else:
                logger.warning(f"Could not find Sheet 2 areas for {filename}, skipping calculations.")
        else:
            if not sheet2:
                logger.warning(f"Sheet 2 not found for {filename}, skipping calculations.")
            if not sheet1_data_area:
                logger.warning(f"Sheet 1 data area not found for {filename}, skipping calculations.")

        workbook.close()
        return combined_data

    except Exception as e:
        logger.error(f"Error processing {filename} for calculations and updates: {str(e)}")
        return {"File Name": filename, "Error": str(e)}


def process_files_parallel(file_paths: List[str], process_func) -> List[Dict[str, Any]]:
    """Process multiple Excel files in parallel using a given processing function."""
    max_workers = min(len(file_paths), MAX_WORKERS)
    results = []

    logger.info(f"Processing {len(file_paths)} files using {max_workers} workers")

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {executor.submit(process_func, fp): fp for fp in file_paths}

        for future in as_completed(future_to_file):
            file_path = future_to_file[future]
            try:
                result = future.result()
                results.append(result)
                logger.info(f"Completed: {os.path.basename(file_path)}")
            except Exception as e:
                logger.error(f"Error processing {file_path}: {e}")
                results.append({"File Name": os.path.basename(file_path), "Error": str(e)})

    return results


# --- Main Orchestration Functions ---

def run_extraction_only():
    """
    Main function for extraction only. Processes files and generates a summary Excel file.
    Does NOT modify original Excel files.
    """
    start_time = time.time()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, DATA_FOLDER)
    output_dir = os.path.join(script_dir, OUTPUT_FOLDER)

    os.makedirs(output_dir, exist_ok=True)

    if not os.path.isdir(data_dir):
        logger.error(f"The directory '{DATA_FOLDER}' was not found.")
        return

    # Get all Excel files
    excel_files = [
        os.path.join(data_dir, f) for f in os.listdir(data_dir)
        if f.endswith(".xlsx") and not f.startswith("~")
    ]

    if not excel_files:
        logger.warning("No valid Excel files (.xlsx) found in the data directory.")
        return

    logger.info(f"Found {len(excel_files)} Excel files to process for extraction only.")

    # Process files using the extraction-only function
    all_results = process_files_parallel(excel_files, process_single_file_extraction_only)

    # Create summary as before
    valid_results = [r for r in all_results if "Error" not in r]
    error_results = [r for r in all_results if "Error" in r]

    if error_results:
        logger.warning(f"{len(error_results)} files had errors during extraction:")
        for error_result in error_results:
            logger.warning(f"  - {error_result['File Name']}: {error_result.get('Error', 'Unknown error')}")

    if valid_results:
        logger.info("Creating output Excel file with extracted data...")

        headers = [
            "File Name", "Document Version", "Inspection Date Finish", "Length Of Inspection (m)",
            "Pipe Material", "Nominal Thickness (in)", "Nominal Thickness (mm)",
            "Operating Pressure (psi)", "Operating Temperature (F)", "Service Fluid",
            "Flange Rating", "Line ID", "NPS (in)", "Total Joint",
            "Minimum Thickness (mm)", "Joint of Minimum Thickness", "Maximum Thickness (mm)",
            "Joint of Maximum Thickness", "Max Thickness Pipe Joint", "Remarks",
            "Above Ground Position", "Lay Down Position", "Under Ground Position",
            "Painting Condition", "Sleeve Condition", "Clamp Condition",
            "Isolation Condition", "Remarks Visual", "Estimation Year Buil", "Pipe Segment"
        ]

        output_workbook = openpyxl.Workbook()
        sheet = output_workbook.active
        sheet.title = "Extraction Summary"
        sheet.append(headers)

        for result in valid_results:
            row_data = []
            for header in headers:
                value = result.get(header, "")
                if isinstance(value, datetime.datetime):
                    value = value.strftime('%Y-%m-%d')
                row_data.append(value)
            sheet.append(row_data)

        output_path = os.path.join(output_dir, "Extraction_Summary.xlsx")
        try:
            output_workbook.save(output_path)
            end_time = time.time()
            processing_time = end_time - start_time

            logger.info(f"Successfully extracted data from {len(valid_results)} files in {processing_time:.2f} seconds")
            logger.info(f"Extraction summary saved to: {output_path}")

        except Exception as e:
            logger.error(f"Error saving extraction summary Excel file: {e}")
    else:
        logger.error("No valid data extracted from any files.")


def run_calculations_and_updates():
    """
    Main function for calculations and updates. Processes files, performs calculations,
    and writes results back to the original Excel files.
    """
    start_time = time.time()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, DATA_FOLDER)
    output_dir = os.path.join(script_dir,
                              OUTPUT_FOLDER)  # Output dir still used for logging, but no new summary file

    os.makedirs(output_dir, exist_ok=True)

    if not os.path.isdir(data_dir):
        logger.error(f"The directory '{DATA_FOLDER}' was not found.")
        return

    # Get all Excel files
    excel_files = [
        os.path.join(data_dir, f) for f in os.listdir(data_dir)
        if f.endswith(".xlsx") and not f.startswith("~")
    ]

    if not excel_files:
        logger.warning("No valid Excel files (.xlsx) found in the data directory.")
        return

    logger.info(f"Found {len(excel_files)} Excel files to process for calculations and updates.")

    # Process files using the calculation and update function
    all_results = process_files_parallel(excel_files, process_single_file_with_calculations_and_updates)

    # Log results of updates
    successful_updates = [r for r in all_results if "Error" not in r]
    failed_updates = [r for r in all_results if "Error" in r]

    if failed_updates:
        logger.warning(f"{len(failed_updates)} files had errors during calculation/update:")
        for error_result in failed_updates:
            logger.warning(f"  - {error_result['File Name']}: {error_result.get('Error', 'Unknown error')}")

    end_time = time.time()
    processing_time = end_time - start_time

    logger.info(
        f"Finished processing {len(successful_updates)} files for calculations and updates in {processing_time:.2f} seconds.")
    logger.info(
        f"Original files in '{DATA_FOLDER}' have been updated with minimum thickness values and inspection rate ratios.")


# Usage example:
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Excel Data Extractor and Calculator.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        '--mode',
        choices=['extract', 'calculate_and_update'],
        default='extract',
        help="""Choose the operation mode:
        'extract' (default): Extracts data from Excel files and creates a summary. Does NOT modify original files.
        'calculate_and_update': Calculates minimum thickness and inspection rate ratio, then writes these to the original Excel files.
        """
    )

    args = parser.parse_args()

    if args.mode == 'extract':
        logger.info("Running in 'extraction only' mode.")
        run_extraction_only()
    elif args.mode == 'calculate_and_update':
        logger.info("Running in 'calculate and update' mode. Original files will be modified.")
        run_calculations_and_updates()
