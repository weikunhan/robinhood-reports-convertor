# -*- coding: utf-8 -*-
# MIT License
#
# Copyright (c) 2024 Weikun Han
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
# ==============================================================================
""" Common utility library

Author:
Weikun Han <weikunhan@gmail.com>
"""

import logging
import json
import os
import pandas as pd
import sys
import time
from typing import Any

def convert_accounting_string_to_float(string_value: str) -> float:
    """Convert accounting string to float

    Args:

    Returns:

    Raises:

    """

    if pd.isnull(string_value):
        return 0.0
    
    if string_value == '':
        return 0.0

    if '$' in string_value:
        string_value = string_value.replace('$', '')

    if '(' in string_value:
        string_value = string_value.replace('(', '')

    if ')' in string_value:
        string_value = string_value.replace(')', '')

    if ',' in string_value:
        string_value = string_value.replace(',', '')    

    return float(string_value)   

def convert_col_type_for_dataframe(
    input_df: pd.DataFrame, column_value: str, type_value: str) -> pd.DataFrame:
    """Convert column type for dataframe

    Args:

    Returns:

    Raises:
        ValueError: if type is not implemented

    """

    if type_value == 'int':
        input_df[column_value] = input_df[column_value].astype(str)
        input_df[column_value] = input_df[column_value].str.replace('S', '')
        input_df[column_value] = pd.to_numeric(
            input_df[column_value], errors='coerce')
        input_df[column_value] = input_df[column_value].fillna(0).astype(int)
    else:
        raise ValueError(f'Invalied type value {type_value}')

    return input_df

def initial_log(log_files_path: str) -> tuple:
    """Initial log with the standard template

    Args:

    Returns:

    Raises:

    """

    logger = logging.getLogger()
    logger_output_filepath = os.path.join(
        log_files_path,
        time.strftime('%Y%m%d_%H%M%S', time.localtime()) + '.log')
    logger_file_handler = logging.FileHandler(logger_output_filepath)
    logger_stream_handler = logging.StreamHandler()
    logger_format_value = ('[%(asctime)s]-[%(processName)s]-[%(threadName)s]'
                           '-[%(levelname)s]: %(message)s')
    logger_stream_handler.setFormatter(logging.Formatter(logger_format_value))
    logger_file_handler.setFormatter(logging.Formatter(logger_format_value))
    logger.addHandler(logger_stream_handler)
    logger.addHandler(logger_file_handler)
    logger.setLevel(logging.INFO)
    return logger, logger_output_filepath

def load_config(input_config_filepath: str, logger: Any) -> dict:
    """Load conifg from input config path

    Args:

    Returns:

    Raises:

    """

    try:
        csv_config_dict = json.loads(open(input_config_filepath).read())
    except Exception as e:
        logger.error(f'Failed config from from: {input_config_filepath}')
        logger.error(f'The detail error message: {e}')
        sys.exit(1) 

    return csv_config_dict

def load_dataframe_from_csv(input_csv_filepath: str, logger: Any) ->  pd.DataFrame:
    """Load dataframe from the csv file

    Args:

    Returns:

    Raises:

    """

    try: 
        input_df = pd.read_csv(input_csv_filepath, 
                               encoding='utf-8', 
                               on_bad_lines='skip', 
                               header=0)
    except Exception as e:
        logger.error(f'Failed read csv from: {input_csv_filepath}')
        logger.error(f'The detail error message: {e}')
        sys.exit(1) 

    return input_df

def load_dataframe_from_excel(
        input_excel_filepath: str, sheet_name_value: str, logger: Any
) ->  pd.DataFrame:
    """Load dataframe from the excel file

    Args:

    Returns:

    Raises:

    """

    try: 
        input_df = pd.read_excel(
            input_excel_filepath, sheet_name=sheet_name_value)
    except Exception as e:
        logger.error(f'Failed read excel from: {input_excel_filepath}')
        logger.error(f'The detail error message: {e}')
        sys.exit(1) 

    return input_df