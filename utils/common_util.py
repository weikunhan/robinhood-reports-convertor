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
import os
import pandas as pd
import time

def convert_col_type_dataframe(
    input_df: pd.DataFrame, column_value: str, type_value: str) -> pd.DataFrame:
    """Convert  column type in dataframe

    Args:

    Returns:

    Raises:
        ValueError: if type is not implemented

    """

    if type_value == 'int':
        input_df[column_value] = pd.to_numeric(
            input_df[column_value], errors='coerce')
        input_df[column_value] = input_df[column_value].fillna(0).astype(int)

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