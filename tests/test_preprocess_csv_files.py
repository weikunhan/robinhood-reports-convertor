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
""" Preprocess CSV files test

Please prepare the ground truth test file as ground_true_data.csv.
Please prepare the target test file as target_test_data.csv.

Author:
Weikun Han <weikunhan@gmail.com>
"""

import logging
import os
import unittest
from utils.common_util import convert_col_type_dataframe
from utils.common_util import convert_string_value
from utils.common_util import initial_log
from utils.common_util import load_dataframe

class TestSimulator(unittest.TestCase):
    """ Preprocess CSV files test"""

    def setUp(self):
        """ Setup test"""

        log_files_path = os.path.join(
                os.path.abspath(os.path.dirname(os.path.dirname(__file__))), 
                'logs')
        data_files_path = os.path.join(
                os.path.abspath(os.path.dirname(os.path.dirname(__file__))), 
                'data')
        true_csv_filepath = os.path.join(data_files_path, 'ground_true_data.csv')
        test_csv_filepath = os.path.join(data_files_path, 'target_test_data.csv')
        self.logger = initial_log(log_files_path)[0]
        self.true_df = load_dataframe(true_csv_filepath, self.logger)
        self.true_df = convert_col_type_dataframe(self.true_df, 'Quantity', 'int')
        self.true_df['Price'] = self.true_df['Price'].apply(convert_string_value)
        self.true_df['Amount'] = self.true_df['Amount'].apply(convert_string_value)
        self.test_df = load_dataframe(test_csv_filepath, self.logger)
        self.test_df = convert_col_type_dataframe(self.test_df, 'Quantity', 'int')
        self.test_df['Price'] = self.test_df['Price'].apply(convert_string_value)
        self.test_df['Amount'] = self.test_df['Amount'].apply(convert_string_value)

    def test_preprocess_csv_files_end_to_end_succeeded(self):
        
        self.logger.info('Testing length for both dataframe are same...')
        
        self.assertEqual(len(self.true_df), len(self.test_df))
        
        self.logger.info('Testing both dataframe have same Activity Date...')

        self.assertTrue((self.true_df.value_counts('Activity Date')
                         .equals(self.test_df.value_counts('Activity Date'))))
        
        self.logger.info('Testing both dataframe have same Process Date...')

        self.assertTrue((self.true_df.value_counts('Process Date')
                         .equals(self.test_df.value_counts('Process Date'))))

        self.logger.info('Testing both dataframe have same Settle Date...')

        self.assertTrue((self.true_df.value_counts('Settle Date')
                         .equals(self.test_df.value_counts('Settle Date'))))

        self.logger.info('Testing both dataframe have same Instrument...')

        self.assertTrue((self.true_df.value_counts('Instrument')
                         .equals(self.test_df.value_counts('Instrument'))))

        self.logger.info('Testing both dataframe have same Description...')

        self.assertTrue((self.true_df.value_counts('Description')
                         .equals(self.test_df.value_counts('Description'))))

        self.logger.info('Testing both dataframe have same Trans Code...')

        self.assertTrue((self.true_df.value_counts('Trans Code')
                         .equals(self.test_df.value_counts('Trans Code'))))

        self.logger.info('Testing both dataframe have equal sum of Quantity...')

        self.assertEqual(
            self.true_df['Quantity'].sum(), self.test_df['Quantity'].sum())

        self.logger.info('Testing both dataframe have equal sum of Price...')

        self.assertEqual(
            self.true_df['Price'].sum(), self.test_df['Price'].sum())

        self.logger.info('Testing both dataframe have equal sum of Amount...')

        self.assertEqual(
            self.true_df['Amount'].sum(), self.test_df['Amount'].sum())

if __name__ == '__main__':
    unittest.main()