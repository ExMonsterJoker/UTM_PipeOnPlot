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
    """Optimized check mark finder."""
    if not header_area or not data_area:
        return "Not Found"

    target_col = find_column_by_keyword_optimized(sheet, header_area, keyword)
    if not target_col:
        return "Not Found"

    data_start_row, data_end_row, _, _ = data_area

    # Read entire column at once
    for row in range(data_start_row, data_end_row + 1):
        cell_val = sheet.cell(row=row, column=target_col).value
        if cell_val and isinstance(cell_val, str) and "√" in cell_val:
            return "√"

    return "Not Found"


def extract_unique_values_from_col_optimized(sheet: Worksheet, data_area: Tuple[int, int, int, int],
                                             col_idx: int) -> str:
    """Optimized unique value extraction."""
    if not data_area or not col_idx:
        return "Not Found"

    data_start_row, data_end_row, _, _ = data_area
    unique_values = set()

    # Read entire column at once
    for row in range(data_start_row, data_end_row + 1):
        cell_val = sheet.cell(row=row, column=col_idx).value
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
    """Optimized data area finder for Sheet 1."""
    # Find start row with value '1' in Column B
    start_row = None
    for r in range(1, min(sheet.max_row + 1, 100)):  # Limit search to first 100 rows
        if sheet.cell(row=r, column=2).value == 1:
            start_row = r
            break

    if not start_row:
        logger.error("Could not find start row with value '1' in Column B.")
        return None

    try:
        total_joint_num = int(total_joint)
    except (ValueError, TypeError):
        logger.error(f"'Total Joint' value ('{total_joint}') is not a valid number.")
        return None

    # Find end row
    end_row = None
    for r in range(start_row, sheet.max_row + 1):
        cell_val = sheet.cell(row=r, column=2).value

        try:
            # Clean up the value if it's a string, then convert to int
            if isinstance(cell_val, str):
                current_joint_num = int(cell_val.strip())
            else:
                current_joint_num = int(cell_val)

            # Check if the current row's joint number matches the target
            if current_joint_num == total_joint_num:
                # It matches. Now, check if the *next* row is different to confirm it's the end.
                next_cell_val = sheet.cell(row=r + 1, column=2).value
                is_last_in_sequence = True  # Assume it's the last one

                try:
                    # Apply the same robust conversion to the next cell's value
                    if isinstance(next_cell_val, str):
                        next_joint_num = int(next_cell_val.strip())
                    else:
                        next_joint_num = int(next_cell_val)

                    # If the next row has the same joint number, it's not the last one
                    if next_joint_num == total_joint_num:
                        is_last_in_sequence = False
                except (ValueError, TypeError, AttributeError):
                    # The next cell is not a valid number, so our current row is the last one.
                    pass

                if is_last_in_sequence:
                    end_row = r
                    break  # Found the end row, so we can exit the loop

        except (ValueError, TypeError, AttributeError):
            # If cell_val is not a number (e.g., text like "JOINT", None, or empty), skip it.
            continue

    if not end_row:
        logger.error(f"Could not find end row matching 'Total Joint' {total_joint_num}.")
        return None

    return start_row, end_row, 2, end_col_from_first_area


def find_areas_visual_optimized(sheet: Worksheet, total_joint: Any, report_type: str) -> Optional[
    Tuple[Tuple[int, int, int, int], Tuple[int, int, int, int]]]:
    """Optimized visual area finder."""
    try:
        search_end_col_idx = column_index_from_string(SEARCH_REMARKS_END_COL)
    except Exception:
        logger.error(f"Invalid column '{SEARCH_REMARKS_END_COL}' in config.")
        return None

    # Find REMARKS cell
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

    # Determine start column based on report type
    start_col_data = 1 if report_type == "v1" else 2

    # Find data start row
    data_start_row = None
    for r in range(1, min(sheet.max_row + 1, 100)):  # Limit search
        if sheet.cell(row=r, column=start_col_data).value == 1:
            data_start_row = r
            break

    if not data_start_row:
        logger.error(f"Could not find data start row with value '1' in Column {get_column_letter(start_col_data)}.")
        return None

    try:
        total_joint_num = int(total_joint)
    except (ValueError, TypeError):
        logger.error(f"'Total Joint' value ('{total_joint}') is not valid.")
        return None

    # Find data end row
    data_end_row = None
    for r in range(data_start_row, sheet.max_row + 1):
        cell_val = sheet.cell(row=r, column=start_col_data).value
        if cell_val is not None and isinstance(cell_val, (int, float)) and int(cell_val) == total_joint_num:
            next_cell_val = sheet.cell(row=r + 1, column=start_col_data).value
            if next_cell_val is None or not isinstance(next_cell_val, (int, float)) or int(
                    next_cell_val) != total_joint_num:
                data_end_row = r
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

    # Read thickness data in batch
    min_thickness_data = []
    for r in range(data_area[0], data_area[1] + 1):
        cell_val = sheet.cell(row=r, column=thickness_col).value
        # Only consider numeric values that are strictly greater than 0
        if isinstance(cell_val, (int, float)) and cell_val > 0:
            min_thickness_data.append({'value': cell_val, 'row': r})

    if not min_thickness_data:
        # This will now be triggered if all values are 0, non-numeric, or blank
        return "Not Found", "Not Found"

    min_item = min(min_thickness_data, key=lambda x: x['value'])
    val_b = sheet.cell(row=min_item['row'], column=2).value
    val_c = sheet.cell(row=min_item['row'], column=3).value
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

    # Read thickness data in batch
    thickness_data = []
    for r in range(data_area[0], data_area[1] + 1):
        cell_val = sheet.cell(row=r, column=thickness_col).value
        if isinstance(cell_val, (int, float)):
            thickness_data.append({'value': cell_val, 'row': r})

    if not thickness_data:
        return "Not Found", "Not Found"

    max_item = max(thickness_data, key=lambda x: x['value'])
    val_b = sheet.cell(row=max_item['row'], column=2).value
    val_c = sheet.cell(row=max_item['row'], column=3).value
    joint = f"{val_b or ''}{val_c or ''}"

    return max_item['value'], joint


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
        data["Total Joint"] = search_for_value_optimized(sheet, header_area, ["RAW DATA INPUT"], offset=2)
        data["Length Of Inspection (m)"] = search_for_value_optimized(sheet, header_area, ["LENGTH"], offset=2)
        data["Nominal Thickness (in)"] = search_for_value_optimized(sheet, header_area, ["NOMINAL THICKNESS"],
                                                                    offset=2, row_offset=1)
        data["Flange Rating"] = find_value_in_cell_optimized(sheet, header_area, "ANSI")
    elif report_type == "v2":
        data["Total Joint"] = search_for_value_optimized(sheet, header_area, ["RAW DATA"], offset=2)
        data["Length Of Inspection (m)"] = search_for_value_optimized(sheet, header_area, ["LENGTH"], offset=1)
        data["Nominal Thickness (mm)"] = search_for_value_optimized(sheet, header_area, ["NOMINAL THICKNESS"],
                                                                    offset=2)
        data["Flange Rating"] = search_for_value_optimized(sheet, header_area, ["ANSI"], row_offset=1)

    # Process data area
    total_joint = data.get("Total Joint")
    data_area = find_data_area_sheet1_optimized(sheet, end_c, total_joint)

    if data_area:
        logger.info(
            f"Data Area: {get_column_letter(data_area[2])}{data_area[0]}:{get_column_letter(data_area[3])}{data_area[1]}")
        header_area_2 = (end_r, data_area[0] - 1, 2, end_c)

        min_thick, min_joint_loc = extract_min_thickness_and_joint_optimized(sheet, data_area, header_area_2)
        data["Minimum Thickness (mm)"] = min_thick
        data["Joint of Minimum Thickness"] = min_joint_loc

        max_thick, max_joint_loc = extract_max_thickness_and_joint_optimized(sheet, data_area, header_area_2)
        data["Maximum Thickness (mm)"] = max_thick
        data["Joint of Maximum Thickness"] = max_joint_loc
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

    # Process each row in the data area
    for row in range(data_area[0], data_area[1] + 1):
        joint_val = sheet.cell(row=row, column=joint_col).value

        if joint_val is not None:
            try:
                joint_num = int(joint_val)

                # Count numeric values in columns D to O
                numeric_count = 0
                for col in range(inspection_start_col, inspection_end_col + 1):
                    cell_val = sheet.cell(row=row, column=col).value

                    # Count only actual numeric values (not None, not empty string)
                    if cell_val is not None and cell_val != "" and isinstance(cell_val, (int, float)):
                        # Count zero as a numeric value (you can change this if needed)
                        numeric_count += 1

                # Calculate ratio using dynamic divisor
                ratio = numeric_count / divisor

                # Store ratio for this joint
                if joint_num not in joint_ratios:
                    joint_ratios[joint_num] = []
                joint_ratios[joint_num].append(ratio)

                logger.debug(f"Joint {joint_num}, Row {row}: {numeric_count} numeric values, ratio = {ratio:.3f}")

            except (ValueError, TypeError):
                logger.warning(f"Invalid joint value at row {row}: {joint_val}")
                continue

    # Calculate average ratio for each joint
    joint_avg_ratios = {}
    for joint_num, ratios in joint_ratios.items():
        if ratios:
            avg_ratio = sum(ratios) / len(ratios)
            joint_avg_ratios[joint_num] = round(avg_ratio, 3)
            logger.info(f"Joint {joint_num}: {len(ratios)} occurrences, average ratio = {avg_ratio:.3f}")

    return joint_avg_ratios

def write_minimum_thickness_to_sheet2(workbook, sheet1, sheet2, sheet1_data_area, sheet2_data_area, sheet1_header_area,
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

    # Collect thickness data by joint from Sheet 1
    joint_thickness_map = {}
    for row in range(sheet1_data_area[0], sheet1_data_area[1] + 1):
        joint_val = sheet1.cell(row=row, column=sheet1_joint_col).value
        thickness_val = sheet1.cell(row=row, column=thickness_col).value

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

    # Determine the column to write thickness values (after data area end column)
    write_col = sheet2_data_area[3] + 1  # Next column after data area end

    # Write header for the new column
    header_row = sheet2_data_area[0] - 1  # Row above data area
    sheet2.cell(row=header_row, column=write_col, value="Min Thickness")

    # Write minimum thickness values to Sheet 2
    written_count = 0
    for row in range(sheet2_data_area[0], sheet2_data_area[1] + 1):
        joint_val = sheet2.cell(row=row, column=sheet2_joint_col).value

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

    # Determine the column to write ratio values (after minimum thickness column)
    # Min thickness is at data_area_end + 1, so ratio goes at data_area_end + 2
    ratio_col = sheet2_data_area[3] + 2  # Two columns after data area end

    # Write header for the new column
    header_row = sheet2_data_area[0] - 1  # Row above data area
    sheet2.cell(row=header_row, column=ratio_col, value="Inspection Rate Ratio")

    # Write ratio values to Sheet 2
    written_count = 0
    for row in range(sheet2_data_area[0], sheet2_data_area[1] + 1):
        joint_val = sheet2.cell(row=row, column=sheet2_joint_col).value

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
    logger.info(
        f"Header Area: {get_column_letter(header_area[2])}{header_area[0]}:{get_column_letter(header_area[3])}{header_area[1]}")
    logger.info(
        f"Data Area: {get_column_letter(data_area[2])}{data_area[0]}:{get_column_letter(data_area[3])}{data_area[1]}")

    data = {
        "Above Ground Position": find_check_mark_in_column_optimized(sheet, header_area, data_area, "Above Ground"),
        "Lay Down Position": find_check_mark_in_column_optimized(sheet, header_area, data_area, "Lay Down"),
        "Under Ground Position": find_check_mark_in_column_optimized(sheet, header_area, data_area, "Under Ground"),
        "Sleeve Condition": find_check_mark_in_column_optimized(sheet, header_area, data_area, "Sleeve"),
        "Clamp Condition": find_check_mark_in_column_optimized(sheet, header_area, data_area, "Clamp"),
        "Isolation Condition": find_check_mark_in_column_optimized(sheet, header_area, data_area, "Isolation"),
    }

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

        report_type = None

        # Rest of the existing code remains the same...
        # Determine report type and get sheets
        sheet1, sheet2 = None, None
        if SHEET_1_NAME_V1 in workbook.sheetnames or SHEET_1_NAME_V1_ALT in workbook.sheetnames:
            report_type = "v1"
            sheet1 = workbook[SHEET_1_NAME_V1] if SHEET_1_NAME_V1 in workbook.sheetnames else workbook[
                SHEET_1_NAME_V1_ALT]
            if SHEET_2_NAME_V1 in workbook.sheetnames:
                sheet2 = workbook[SHEET_2_NAME_V1]
        elif SHEET_1_NAME_V2 in workbook.sheetnames:
            report_type = "v2"
            sheet1 = workbook[SHEET_1_NAME_V2]
            if SHEET_2_NAME_V2 in workbook.sheetnames:
                sheet2 = workbook[SHEET_2_NAME_V2]

        if not report_type:
            logger.warning(f"Could not determine report type for {filename}")
            workbook.close()
            return {"File Name": filename, "Error": "Unknown report type"}

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
    and writes the results back to the original file.
    """
    filename = os.path.basename(file_path)
    logger.info(f"Processing file for calculations and updates: {filename}")

    try:
        # Load workbook in write mode (not read_only)
        workbook = openpyxl.load_workbook(file_path, read_only=False, data_only=True)

        combined_data = {"File Name": filename}
        report_type = None

        # Determine report type and get sheets
        sheet1, sheet2 = None, None
        if SHEET_1_NAME_V1 in workbook.sheetnames or SHEET_1_NAME_V1_ALT in workbook.sheetnames:
            report_type = "v1"
            sheet1 = workbook[SHEET_1_NAME_V1] if SHEET_1_NAME_V1 in workbook.sheetnames else workbook[
                SHEET_1_NAME_V1_ALT]
            if SHEET_2_NAME_V1 in workbook.sheetnames:
                sheet2 = workbook[SHEET_2_NAME_V1]
        elif SHEET_1_NAME_V2 in workbook.sheetnames:
            report_type = "v2"
            sheet1 = workbook[SHEET_1_NAME_V2]
            if SHEET_2_NAME_V2 in workbook.sheetnames:
                sheet2 = workbook[SHEET_2_NAME_V2]

        if not report_type:
            logger.warning(f"Could not determine report type for {filename}")
            workbook.close()
            return {"File Name": filename, "Error": "Unknown report type"}

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
            "Minimum Thickness (mm)", "Joint of Minimum Thickness","Maximum Thickness (mm)","Joint of Maximum Thickness", "Remarks",
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
    output_dir = os.path.join(script_dir, OUTPUT_FOLDER) # Output dir still used for logging, but no new summary file

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

    logger.info(f"Finished processing {len(successful_updates)} files for calculations and updates in {processing_time:.2f} seconds.")
    logger.info(f"Original files in '{DATA_FOLDER}' have been updated with minimum thickness values and inspection rate ratios.")


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

