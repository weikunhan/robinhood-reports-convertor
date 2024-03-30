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
""" Preprocess CSV files

Welcome to use this to preprocess CSV files!
Please check the README file if have any question.

Author:
Weikun Han <weikunhan@gmail.com>
"""

import argparse
import os
import pandas as pd
import tqdm
import typing
from utils.common_util import convert_col_type_for_dataframe
from utils.common_util import initial_log
from utils.common_util import load_config
from utils.common_util import load_dataframe_from_csv

CSV_PREPROCESS_CONFIG_PATCH = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), 
    'configs',
    'csv_proprocess_config.json')

def get_non_overlap_dataframe(
    first_df: pd.DataFrame, second_df: pd.DataFrame, logger: typing.Any
) -> pd.DataFrame:
    """Get non overlap dataframe after compare last and first rows

    Assume that first_df have more recently (new) data than second_df

    Args:

    Returns:

    Raises:

    """

    for i in tqdm.tqdm(range(1, len(second_df) + 1), 
                       desc='Validating in progress'):
        temp_first_df = first_df.iloc[-i:len(first_df)].copy()
        temp_first_df.sort_values(
            by=['Instrument', 'Amount', 'Price', 'Quantity'], inplace=True)
        temp_first_df.reset_index(drop=True, inplace=True)
        temp_second_df = second_df.iloc[0:i].copy()
        temp_second_df.sort_values(
            by=['Instrument', 'Amount', 'Price', 'Quantity'], inplace=True)
        temp_second_df.reset_index(drop=True, inplace=True)

        if temp_first_df.equals(temp_second_df):
            msg = (f'Found the total overlap rows in this report is: {i}\n')
            logger.warning(msg)
            return second_df.drop(second_df.index[0:i])

    logger.info('Not found the overlap rows in this report\n')
    return second_df

def save_first_result(
    input_df_list: list, output_csv_filepath: str, logger: typing.Any
) -> None:
    """Save result after combination without overlap rows 

    Args:

    Returns:

    Raises:

    """

    count = 2
    output_df = input_df_list[0]

    for first_df, second_df in zip(input_df_list[:-1], input_df_list[1:]):
        logger.info(f'Processing first part report number is: {count}...\n')
        filtered_df = get_non_overlap_dataframe(first_df, second_df, logger)
        output_df = pd.concat([output_df, filtered_df], ignore_index=True)
        count += 1

    output_df.to_csv(output_csv_filepath, index=False)

def save_reset_result(
    input_df_list: list, output_csv_filepath: str, logger: typing.Any
) -> None:
    """Save result after combination without overlap rows 

    Args:

    Returns:

    Raises:

    """

    count = 1
    output_df = pd.DataFrame()

    for first_df, second_df in zip(input_df_list[:-1], input_df_list[1:]):
        logger.info(f'Processing reset part report number is: {count}...\n')
        filtered_df = get_non_overlap_dataframe(first_df, second_df, logger)
        output_df = pd.concat([output_df, filtered_df], ignore_index=True)
        count += 1

    output_df.to_csv(output_csv_filepath, mode='a', header=False, index=False)

def main ():   

    if not os.path.exists(args.data_files_path):
        os.makedirs(args.data_files_path)

    if not os.path.exists(args.log_files_path):
        os.makedirs(args.log_files_path)

    logger, logger_output_filepath = initial_log(args.log_files_path)    
    output_csv_filepath = os.path.join(args.data_files_path, args.output_csv_name)  
    logger.info('=' * 80)
    logger.info('Start preprocess CSV files')
    logger.info(f'The csv files load from: {args.data_files_path}')
    logger.info(f'The log file saved into: {logger_output_filepath}')
    logger.info('=' * 80 + '\n')

    if os.path.exists(output_csv_filepath):
        os.remove(output_csv_filepath)

    csv_config_dict = load_config(CSV_PREPROCESS_CONFIG_PATCH, logger)
    csv_config_dict = {key: value for key, value in csv_config_dict.items() 
                       if isinstance(value, list)}
    csv_config_dict = {key: csv_config_dict[key] 
                       for key in sorted(csv_config_dict.keys())}
    last_df = None

    for key, value in csv_config_dict.items():
        msg = ('Loading robinhood stock and option reports for part '
               f'{key}: {value}...\n')
        logger.info(msg)
        input_df_list = [] if last_df is None else [last_df]

        for csv_info_value in value:
            input_df = load_dataframe_from_csv(
                os.path.join(args.data_files_path, csv_info_value), logger)
            input_df = convert_col_type_for_dataframe(
                input_df, 'Quantity', 'int')  
            input_df.drop(input_df.index[-1], inplace=True)       
            input_df_list.append(input_df) # type: ignore

        last_df = input_df_list[-1]

        msg = ('Saving concated robinhood stock and option reports for part '
               f'{key}: {value}...\n')
        logger.info(msg)

        if os.path.exists(output_csv_filepath):
            save_reset_result(input_df_list, output_csv_filepath, logger)
        else:
            save_first_result(input_df_list, output_csv_filepath, logger)

    logger.info('=' * 80)
    logger.info('Finished preprocess CSV files')
    logger.info(f'The csv file saved into: {output_csv_filepath}')
    logger.info(f'The log file saved into: {logger_output_filepath}')
    logger.info('=' * 80 + '\n')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Preprocess CSV files')
    parser.add_argument('-o', '--output-csv-name', type=str,
                        required=True,
                        help='Output report file name')
    parser.add_argument('-d', '--data-files-path', type=str,
                        default=os.path.join(
                            os.path.abspath(os.path.dirname(__file__)), 'data'),
                        required=False,
                        help='Data files input and output path')
    parser.add_argument('-l', '--log-files-path', type=str,
                        default=os.path.join(
                            os.path.abspath(os.path.dirname(__file__)), 'logs'),
                        required=False,
                        help='Log files save path')
    args = parser.parse_args()

    print('-' * 80 + '\n')
    print('Welcome to use this to preprocess CSV files!\n')
    print('Please check the README file if have any question.\n')
    print('-' * 80 + '\n')
    main()