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
""" Robinhood reports convertor

Welcome to use the Robinhood reports convertor!
Please check the README file if have any question.

Author:
Weikun Han <weikunhan@gmail.com>
"""

import argparse
import os
import pandas as pd
import re
import tqdm
import typing
from collections import defaultdict
from utils.common_util import convert_accounting_string_to_float
from utils.common_util import convert_common_instrument_to_one
from utils.common_util import convert_col_type_for_dataframe
from utils.common_util import convert_date_to_standard_format
from utils.common_util import initial_log
from utils.common_util import load_config
from utils.common_util import load_dataframe_from_csv

STOCK_EXCEL_COL_NAME_LIST = [
    'Date', 'Type', 'Quantity', 'Price', 'Amount', 'Profit']
OPTION_EXCEL_COL_NAME_LIST = [
    'Date', 'Description', 'Type', 'Quantity', 'Price', 'Amount', 'Profit']    
INSTRUMENT_TRANSCODE_CONFIG_PATCH = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), 
    'configs',
    'instrument_transcode_config.json'
)

def calculate_quantity_value_from_balance(
    instrument_config_dict: dict, 
    position_data_dict: dict,
    transcode_value:str,
    description_value: str,
    quantity_value: int,
    balance_quantity_value: int
) -> int:
    """Calculates quantity value from balance

    Args:
        instrument_config_dict: A dictionary for configuration settings
        position_data_dict: A dictionary containing the option position 
        transcode_value: The sting of transcode for the option
        description_value: The sting of description for the option
        quantity_value: The int of quantity for the option
        balance_quantity_value: The int of balance quantity for the option

    Returns:
        The int value calculated quantity value from balance
    """

    if (
        instrument_config_dict["option"][transcode_value][0] in (1, 3) 
        and position_data_dict[description_value][0] > 0
    ): 
        return int(balance_quantity_value - quantity_value)
    
    return int(balance_quantity_value + quantity_value)

def calculate_quantity_value_from_record(
    transcode_value: str,
    quantity_value: int, 
    record_quantity_value: int
) -> int:
    """Calculates quantity value from record

    Args:
        transcode_value: The sting of transcode for the option
        quantity_value: The int of quantity for the option
        record_quantity_value: The int of record quantity for the option

    Returns:
        The int value calculated quantity value from record
    """
    
    if transcode_value in ('BTO', 'STC'):
        return int(quantity_value + record_quantity_value)
    elif transcode_value in ('BTC', 'STO'):
        return int(quantity_value - record_quantity_value)
    else:
        return int(0)
    
def get_option_dict(
    instrument_config_dict: dict,
    option_data_dict: dict,
    date_value: str,
    description_value: str,
    transcode_value: str,
    quantity_value: int,
    price_value: float,
    amount_value: float
) -> tuple:
    """Get option dict

    For the transcode value as group 1, we need handle it in result part. If the 
    final position is negative, the factor value is 1; otherwise, it is -1. And 
    the amount value is none.

    For the transcode value as group 2, the factor value is used as config. And 
    the amount value is none.

    For the transcode value as group 0, the factor value is used as config. And 
    the quantity value increases as the amount value decreases; 
    otherwise, vice versa

    Args:
        instrument_config_dict: A dictionary for configuration settings
        option_data_dict: A dictionary containing the option data to save
        date_value: The sting of date for the option
        description_value: The sting of description for the option
        transcode_value: The sting of transcode for the option
        quantity_value: The int of quantity for the option
        price_value: The float of price for the option
        amount_value: The float of amount for the option

    Returns:
        A tuple containing the option dictionary and any relevant metadata

    Raises:
        None
    """   

    factor_value: int = instrument_config_dict['option'][transcode_value][1]
    key_value = f'{date_value}*{transcode_value}*{price_value}*{description_value}' 

    if instrument_config_dict['option'][transcode_value][0] in (1, 2, 3):
        return (
            key_value, 
            int(option_data_dict[key_value][0] + factor_value * quantity_value),
            float(0)
        )
    else:
        return (
            key_value, 
            int(option_data_dict[key_value][0] + factor_value * quantity_value),
            float(option_data_dict[key_value][1] - factor_value * amount_value)
        )

def get_stock_dict(
    instrument_config_dict: dict,
    stock_data_dict: dict,
    date_value: str,
    transcode_value: str,
    quantity_value: int,
    price_value: float,
    amount_value: float,
    day_trade_value: int
) -> tuple:
    """Get stock dict

    For the transcode value as group 1; if the current quantity value is 0, the 
    factor value is 1; otherwise, it is -1. And the amount value is none.

    For the transcode value as group 2, the factor value is used as config. And 
    the quantity value is none.

    For the transcode value as group 0, the factor value is used as config. And 
    the quantity value increases as the amount value decreases; 
    otherwise, vice versa

    Args:
        instrument_config_dict: A dictionary for configuration settings
        stock_data_dict: A dictionary containing the stock data to save
        date_value: The sting of date for the stock
        transcode_value: The sting of transcode for the stock
        quantity_value: The int of quantity for the stock
        price_value: The float of price for the stock
        amount_value: The float of amount for the stock
        day_trade_value: The int of day trade for the stock

    Returns:
        A tuple containing the stock dictionary and any relevant metadata
        
    Raises:
        None
    """   

    factor_value: int = instrument_config_dict['stock'][transcode_value][1]
    key_value = f'{date_value}*{transcode_value}*{price_value}*{day_trade_value}' 
 
    if instrument_config_dict['stock'][transcode_value][0] == 1:
        if stock_data_dict[key_value][0] == 0:
            return (
                key_value, 
                int(stock_data_dict[key_value][0] + factor_value * quantity_value),
                float(0)
            )
        else:
            return (
                key_value, 
                int(stock_data_dict[key_value][0] - factor_value * quantity_value),
                float(0)
            )
    elif instrument_config_dict['stock'][transcode_value][0] == 2:
        return (
            key_value, 
            int(0),
            float(stock_data_dict[key_value][1] + factor_value * amount_value)
        )
    else:
        return (
            key_value, 
            int(stock_data_dict[key_value][0] + factor_value * quantity_value),
            float(stock_data_dict[key_value][1] - factor_value * amount_value)
        )

def get_stock_and_option_dict(
    instrument_config_dict: dict,
    instrument_df: pd.DataFrame,
    instrument_value: str,
    logger: typing.Any
) -> tuple:
    """Get stock and option dict from grouped dataframe

    From Python 3.7, the dict also maintains insertion order.

    Args:
        instrument_config_dict: A dictionary for configuration settings
        instrument_df: The DataFrame containing instrument data
        instrument_value: The string of value identifying the instrument
        logger: An object for logging messages

    Returns:
        A tuple containing two dictionaries: stock_dict and option_dict

    Raises:
        None
    """

    stock_data_dict: defaultdict[str, typing.Tuple[int, float]] = defaultdict(
        lambda: (0, 0.0))
    option_data_dict: defaultdict[str, typing.Tuple[int, float]] = defaultdict(
        lambda: (0, 0.0))
    day_trade_dict: dict = {}

    for row in tqdm.tqdm(
        instrument_df.itertuples(index=False), 
        total=len(instrument_df),
        desc='Converting in progress'
    ):
        date_value = row[0]
        description_value = re.split(rf'(?={instrument_value}\d*)', row[4])[-1]
        transcode_value = row[5]
        quantity_value = row[6]
        price_value = row[7]
        amount_value = row[8]

        if transcode_value in instrument_config_dict['stock']:
            if date_value in day_trade_dict:
                if transcode_value != day_trade_dict[date_value][-1][0]:
                    day_trade_dict[date_value].append(
                        (transcode_value, 
                         day_trade_dict[date_value][-1][1] + 1)
                    )
            else:
                day_trade_dict[date_value] = [(transcode_value, 1)]

            key, quantity_sum_value, amount_sum_value = get_stock_dict(
                instrument_config_dict, 
                stock_data_dict,
                date_value, 
                transcode_value, 
                quantity_value,
                price_value,
                amount_value,
                day_trade_dict[date_value][-1][1]
            )
            stock_data_dict[key] = (quantity_sum_value, amount_sum_value)
        elif transcode_value in instrument_config_dict['option']:  
            key, quantity_sum_value, amount_sum_value = get_option_dict(
                instrument_config_dict, 
                option_data_dict,
                date_value,       
                description_value.strip(),
                transcode_value, 
                quantity_value,
                price_value,
                amount_value
            )
            option_data_dict[key] = (quantity_sum_value, amount_sum_value)
        else:
            logger.warning(f'Found undefined transcode: {transcode_value}\n')

    return stock_data_dict, option_data_dict    

def save_option_result(
    instrument_config_dict: dict, 
    option_data_dict: dict, 
    output_xlsx_filepath: str, 
    logger: typing.Any
) -> None:   
    """Save option result into special format

    Args:
        instrument_config_dict: A dictionary for configuration settings
        option_data_dict: A dictionary containing the option data to save
        output_xlsx_filepath:  The string of XLSX saving file path
        logger: An object for logging messages

    Returns:
        None

    Raises:
        None
    """

    position_data_dict: defaultdict[str, typing.Tuple[int, float]] = defaultdict(
        lambda: (0, 0.0))
    position_data_set: set = set() 
    option_data_list: list = []

    for key, value in reversed(option_data_dict.items()):
        date_value = key.split('*')[0]
        transcode_value = key.split('*')[1]
        price_value = key.split('*')[2]
        description_value = key.split('*')[3]
        quantity_value = value[0]
        amount_value = value[1]
        
        if description_value in position_data_set:
            position_data_dict[description_value] = (
                calculate_quantity_value_from_record(
                    transcode_value,
                    quantity_value,
                    position_data_dict[description_value][0]
                ),
                amount_value
            )
            position_data_set.remove(description_value)
        else:
            if (
                instrument_config_dict["option"][transcode_value][0] == 3
                and description_value not in position_data_dict
            ):
                position_data_set.add(description_value)

            position_data_dict[description_value] = (
                calculate_quantity_value_from_balance(
                    instrument_config_dict,
                    position_data_dict,
                    transcode_value,
                    description_value,
                    quantity_value,
                    position_data_dict[description_value][0]),
                position_data_dict[description_value][1] + amount_value
            )

        if position_data_dict[description_value][0] != 0:
            option_data_list.append(
                [str(date_value),
                 str(description_value),
                 str(transcode_value),
                 int(quantity_value),
                 float(price_value),
                 float(amount_value),
                 None]
            )
        else:
            logger.info(f'Calculating profit on date: {date_value}...\n')
            option_data_list.append(
                [str(date_value),
                 str(description_value),
                 str(transcode_value),
                 int(quantity_value),
                 float(price_value),
                 float(amount_value),
                 float(position_data_dict[description_value][1])]
            )
            position_data_dict[description_value] = (0, 0.0)

    option_df = pd.DataFrame(
        option_data_list, columns=OPTION_EXCEL_COL_NAME_LIST) 

    with pd.ExcelWriter(output_xlsx_filepath, mode='a') as writer:
        option_df.to_excel(writer, sheet_name='OPTION', index=False)

def save_stock_result(
    instrument_config_dict: dict,
    stock_data_dict: dict, 
    output_xlsx_filepath: str, 
    logger: typing.Any,
) -> None:   
    """Save stock result into special format

    Args:
        instrument_config_dict: A dictionary for configuration settings
        stock_data_dict:  A dictionary containing the stock data to save
        output_xlsx_filepath:  The string of XLSX saving file path
        logger: An object for logging messages

    Returns:
        None

    Raises:
        None
    """

    quantity_sum_value: int = 0
    amount_sum_value: float = 0.0
    stock_data_list: list = []
        
    for key, value in reversed(stock_data_dict.items()):
        date_value = key.split('*')[0]
        transcode_value = key.split('*')[1]
        price_value = key.split('*')[2]
        quantity_value = value[0]
        amount_value = value[1]

        if instrument_config_dict['stock'][transcode_value][0] == 2: 
            stock_data_list.append(
                [str(date_value),
                 str(transcode_value), 
                 int(0), 
                 float(0),
                 float(amount_value), 
                 float(amount_value)]
            )
        else:
            quantity_sum_value += quantity_value
            amount_sum_value += amount_value

            if quantity_sum_value != 0:
                stock_data_list.append(
                    [str(date_value),
                     str(transcode_value),
                     int(quantity_value),
                     float(price_value), 
                     float(amount_value), 
                     None]
                )
            else:
                logger.info(f'Calculating profit on date: {date_value}...\n')
                stock_data_list.append(
                    [str(date_value),
                     str(transcode_value),
                     int(quantity_value),
                     float(price_value),
                     float(amount_value),
                     float(amount_sum_value)]
                )
                amount_sum_value = 0.0
   
    stock_df = pd.DataFrame(stock_data_list, columns=STOCK_EXCEL_COL_NAME_LIST) 

    with pd.ExcelWriter(output_xlsx_filepath) as writer:
        stock_df.to_excel(writer, sheet_name='STOCK', index=False)

def main ():

    if not os.path.exists(args.data_files_path):
        os.makedirs(args.data_files_path)

    if not os.path.exists(args.log_files_path):
        os.makedirs(args.log_files_path)

    logger, logger_output_filepath = initial_log(args.log_files_path)
    input_csv_filepath = os.path.join(args.data_files_path, args.input_csv_name)
    logger.info('=' * 80)
    logger.info('Start convertor execution')
    logger.info(f'The csv file load from: {input_csv_filepath}')
    logger.info(f'The log file saved into: {logger_output_filepath}')
    logger.info('=' * 80 + '\n')
    instrument_config_dict = load_config(
        INSTRUMENT_TRANSCODE_CONFIG_PATCH, logger)
    input_df = load_dataframe_from_csv(input_csv_filepath, logger)
    input_df = convert_col_type_for_dataframe(input_df, 'Quantity', 'int')
    input_df['Activity Date'] = (
        input_df['Activity Date'].apply(convert_date_to_standard_format))
    input_df['Instrument'] = (
        input_df['Instrument'].apply(convert_common_instrument_to_one))
    input_df['Price'] = (
        input_df['Price'].apply(convert_accounting_string_to_float))
    input_df['Amount'] = (
        input_df['Amount'].apply(convert_accounting_string_to_float))

    for instrument_value, instrument_df in input_df.groupby('Instrument'):
        logger.info(
            'Converting robinhood stock and option reports for: '
            f'{instrument_value}...\n'
        )
        stock_data_dict, option_data_dict = get_stock_and_option_dict(
            instrument_config_dict, 
            instrument_df, 
            str(instrument_value), 
            logger
        )
        logger.info(f'Saving stock result for: {instrument_value}...\n')
        save_stock_result(
            instrument_config_dict,
            stock_data_dict,
            os.path.join(args.data_files_path, f'{instrument_value}.xlsx'),
            logger
        )
        logger.info(f'Saving option result for: {instrument_value}...\n')
        save_option_result(
            instrument_config_dict,
            option_data_dict,
            os.path.join(args.data_files_path, f'{instrument_value}.xlsx'),
            logger
        )

    logger.info('=' * 80)
    logger.info('Finished convertor execution')
    logger.info(f'The xlsx files saved into: {args.data_files_path}')
    logger.info(f'The log file saved into: {logger_output_filepath}')
    logger.info('=' * 80 + '\n')
               
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Robinhood reports convertor')
    parser.add_argument(
        '-i', 
        '--input-csv-name', 
        type=str,
        required=True,
        help='Input report file name')
    parser.add_argument(
        '-d', 
        '--data-files-path', 
        type=str,
        default=os.path.join(os.path.abspath(os.path.dirname(__file__)), 'data'),
        required=False,
        help='Data files input and output path')
    parser.add_argument(
        '-l', 
        '--log-files-path', 
        type=str,
        default=os.path.join(os.path.abspath(os.path.dirname(__file__)), 'logs'),
        required=False,
        help='Log files save path')
    args = parser.parse_args()

    print('-' * 80 + '\n')
    print('Welcome to use the Robinhood reports convertor!\n')
    print('Please check the README file if have any question.\n')
    print('-' * 80 + '\n')
    main()