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
""" Test preprocess CSV files

Please prepare the ground truth test file as ground_true_data.csv.
Please prepare the target test file as target_test_data.csv.

Author:
Weikun Han <weikunhan@gmail.com>
"""

import os
import unittest
from utils.common_util import convert_accounting_string_to_float
from utils.common_util import convert_common_instrument_to_one
from utils.common_util import convert_col_type_for_dataframe
from utils.common_util import convert_date_to_standard_format
from utils.common_util import initial_log
from utils.common_util import load_dataframe_from_csv

class TestPreprocessCsvFiles(unittest.TestCase):
    """Test preprocess CSV files"""

    def setUp(self):
        """Setup test"""

        log_files_path = os.path.join(
            os.path.abspath(os.path.dirname(os.path.dirname(__file__))), 'logs')
        data_files_path = os.path.join(
            os.path.abspath(os.path.dirname(os.path.dirname(__file__))), 'data')
        true_csv_filepath = os.path.join(data_files_path, 'ground_true_data.csv')
        test_csv_filepath = os.path.join(data_files_path, 'target_test_data.csv')
        self.logger = initial_log(log_files_path)[0]
        self.true_df = load_dataframe_from_csv(true_csv_filepath, self.logger)
        self.true_df = convert_col_type_for_dataframe(
            self.true_df, 'Quantity', 'int')
        self.true_df['Activity Date'] = (
            self.true_df['Activity Date'].apply(convert_date_to_standard_format))
        self.true_df['Process Date'] = (
            self.true_df['Process Date'].apply(convert_date_to_standard_format))
        self.true_df['Settle Date'] = (
            self.true_df['Settle Date'].apply(convert_date_to_standard_format))
        self.true_df['Instrument'] = (
            self.true_df['Instrument'].apply(convert_common_instrument_to_one))
        self.true_df['Price'] = (
            self.true_df['Price'].apply(convert_accounting_string_to_float))
        self.true_df['Amount'] = (
            self.true_df['Amount'].apply(convert_accounting_string_to_float))
        self.test_df = load_dataframe_from_csv(test_csv_filepath, self.logger)
        self.test_df = convert_col_type_for_dataframe(
            self.test_df, 'Quantity', 'int')
        self.test_df['Activity Date'] = (
            self.test_df['Activity Date'].apply(convert_date_to_standard_format))
        self.test_df['Process Date'] = (
            self.test_df['Process Date'].apply(convert_date_to_standard_format))
        self.test_df['Settle Date'] = (
            self.test_df['Settle Date'].apply(convert_date_to_standard_format))
        self.test_df['Instrument'] = (
            self.test_df['Instrument'].apply(convert_common_instrument_to_one))
        self.test_df['Price'] = (
            self.test_df['Price'].apply(convert_accounting_string_to_float))
        self.test_df['Amount'] = (
            self.test_df['Amount'].apply(convert_accounting_string_to_float))

    def test_preprocess_csv_files_end_to_end_succeeded(self):
        """Test preprocess CSV files end to end succeeded"""

        self.logger.info('=' * 80)
        self.logger.info(
            'Start testing preprocess CSV files end to end succeeded')
        self.logger.info('=' * 80 + '\n')
        self.logger.info('Testing length for both dataframe are same...\n')
        
        self.assertEqual(
            len(self.true_df), len(self.test_df), 'Both length should be same')
        
        self.logger.info('Testing both dataframe have same Activity Date...\n')

        self.assertTrue(
            (self.true_df.value_counts('Activity Date')
             .equals(self.test_df.value_counts('Activity Date'))),
             'Both Activity Date count shoule be same'
        )
        
        self.logger.info('Testing both dataframe have same Process Date...\n')

        self.assertTrue(
            (self.true_df.value_counts('Process Date')
            .equals(self.test_df.value_counts('Process Date'))),
            'Both Process Date count shoule be same'
        )

        self.logger.info('Testing both dataframe have same Settle Date...\n')

        self.assertTrue(
            (self.true_df.value_counts('Settle Date')
            .equals(self.test_df.value_counts('Settle Date'))),
            'Both Settle Date count shoule be same'
        )

        self.logger.info('Testing both dataframe have same Instrument...\n')

        self.assertTrue(
            (self.true_df.value_counts('Instrument')
            .equals(self.test_df.value_counts('Instrument'))),
            'Both Instrument count shoule be same'
        )

        self.logger.info('Testing both dataframe have same Description...\n')

        self.assertTrue(
            (self.true_df.value_counts('Description')
            .equals(self.test_df.value_counts('Description'))),
            'Both Description count shoule be same'
        )

        self.logger.info('Testing both dataframe have same Trans Code...\n')

        self.assertTrue(
            (self.true_df.value_counts('Trans Code')
            .equals(self.test_df.value_counts('Trans Code'))),
            'Both Trans Code count shoule be same'
        )

        self.logger.info('Testing both dataframe have equal sum of Quantity...\n')

        self.assertEqual(
            self.true_df['Quantity'].sum(), 
            self.test_df['Quantity'].sum(),
            'Both Quantity sum shoule be same'
        )

        self.logger.info('Testing both dataframe have equal sum of Price...\n')

        self.assertEqual(
            self.true_df['Price'].sum(), 
            self.test_df['Price'].sum(),
            'Both Price sum shoule be same'
        )

        self.logger.info('Testing both dataframe have equal sum of Amount...\n')

        self.assertEqual(
            self.true_df['Amount'].sum(), 
            self.test_df['Amount'].sum(),
            'Both Amount sum shoule be same'
        )

        self.logger.info('=' * 80)
        self.logger.info(
            'Finihsed testing preprocess CSV files end to end succeeded')
        self.logger.info('=' * 80 + '\n')

if __name__ == '__main__':
    unittest.main()