# -*- coding: utf-8 -*-
# ==============================================================================
""" xxx

xxx

Author:
xxx

Requirement:
"""

import argparse
from ast import Lambda
from re import I
from statistics import quantiles
import pandas as pd
import numpy as np
from collections import defaultdict
import logging
import os
import time
from openpyxl.styles import NamedStyle
from openpyxl import load_workbook

EXCEL_COL_NAME_LIST = ['Date', 'Quantity', 'Price', 'Amount']
FORMAT_DICT = {'Date': lambda x: f'{x.striftime("%d/%m/%Y")}',
               'Price': '${0:.02f}',
               'Amount': '${0:.02f}'}

def initial_log() -> tuple:
    """Initial log with the standard template

    Args:

    Returns:

    Raises:

    """

    logger = logging.getLogger()
    logger_output_filepath = os.path.join(
        args.log_files_path,
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



def main ():

    file_path = './output.xlsx'

    if os.path.exists(file_path):
        os.remove(file_path)
    
    stock_df = pd.read_csv('./test.csv', encoding='utf-8', on_bad_lines='skip', header=0)

    unique_stock_list = stock_df['Instrument'].dropna().unique()

    #header = stock_df.columns.tolist()
    stock_df['Quantity'] = pd.to_numeric(stock_df['Quantity'], errors='coerce')
    stock_df['Quantity'] = stock_df['Quantity'].fillna(0).astype(int)

    
    for stock_value in unique_stock_list[:1]:
        data_dict = defaultdict(lambda: (0, 0))
        data_list = []

        for index, row in stock_df.iterrows():
            if stock_value == row['Instrument']:
                amount_value = row['Amount'].replace('$', '').replace('(', '').replace(')', '').replace(',', '')
                
                if 'AFEE' == row['Trans Code']:
                    key_tuple = row['Activity Date'], '0'
                    data_dict[key_tuple] = (
                        0,
                        data_dict[key_tuple][1] - float(amount_value)) 
                elif 'Buy' == row['Trans Code']:
                    key_tuple = row['Activity Date'], row['Price'].replace('$', '')
                    data_dict[key_tuple] = (
                        data_dict[key_tuple][0] + int(row['Quantity']),
                        data_dict[key_tuple][1] - float(amount_value))     
                elif 'Sell' == row['Trans Code']:
                    key_tuple = row['Activity Date'], row['Price'].replace('$', '')
                    data_dict[key_tuple] = (
                        data_dict[key_tuple][0] - int(row['Quantity']),
                        data_dict[key_tuple][1] + float(amount_value))   
                else:
                    print(f'found new value: {row["Trans Code"]}')
                
 

        print('++++++')

        # create new data frame
        for key, value in reversed(data_dict.items()):
        
            data_list.append([key[0], value[0], float(key[1]), value[1]])

        result_df = pd.DataFrame(data_list, columns=EXCEL_COL_NAME_LIST)   
       #result_df['Date'] = pd.to_datetime(result_df['Date'], format='%m/%d/%Y').dt.date
        

        if os.path.exists(file_path):
            with pd.ExcelWriter(file_path, mode='a') as writer:
                result_df.to_excel(writer, sheet_name=stock_value, index=False)
        else:
            with pd.ExcelWriter(file_path) as writer:
                result_df.to_excel(writer, sheet_name=stock_value, index=False)
               

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Scan DB Home image name mismatch issue PROD & DEMO')
    parser.add_argument('-c', '--csv-files-path', type=str,
                        default=os.path.expanduser('~/csv_files'),
                        required=False,
                        help='CSV files save path')
    parser.add_argument('-l', '--log-files-path', type=str,
                        default=os.path.expanduser('~/logs'),
                        required=False,
                        help='Log files save path')
    parser.add_argument('-r1', '--report-output-name-1', type=str,
                        default='report_test.csv',
                        required=False,
                        help='Report file 1 csv name')
    args = parser.parse_args()

    print('-' * 80 + '\n')
    print('Please run command to get authentication before using this script:\n')
    print('oci session authenticate --profile-name DEFAULT --region us-ashburn-1\n')
    print('-' * 80 + '\n')
    main()

