import os 
# Api key has been place at github secret environment.
# We do not have to enter api key when testing.
# os.environ['TEJAPI_KEY'] = os.environ.get('TQUANTLABTESTKEY')
print('test_file:', os.getenv('TEJAPI_KEY'))
print('test_file:', os.getenv('TEJAPI_BASE'))

# import pytest

# def pytest_configure(config):
#     os.environ['TEJAPI_KEY'] = os.environ.get('TQUANTLABTESTKEY')

import sys
print(os.path.pardir)
sys.path.insert(0, os.path.pardir)
print(sys.path)

# import TejToolAPI
from TejToolAPI import get_history_data
from TejToolAPI import parameters as para

import pandas as pd
from zipline.data.data_portal import get_fundamentals
from zipline.sources.TEJ_Api_Data import get_universe


# from TejToolAPI import TejToolAPI


def test_get_universe():
        start = '2013-01-01' # os.getenv('start_date')
        end = '2023-01-01' # os.getenv('end_date')
        pool = get_universe(start, end, mkt = ['TWSE', 'OTC'], stktp_e = 'Common Stock')
        return pool

def assert_date(data, start, end):

    assert data['mdate'].min() == pd.Timedelta(start) and data['mdate'].max() == pd.Timedelta(end)
 

class UnitTest:
    def setup_method(self):
        # Get stock tickers from get_universe in zipline-tej
        self.stock_univers = test_get_universe()
        
        # Get all columns from TQuant lab datasets.
        self.all_columns = para.table_columns['COLUMNS'].unique().tolist()
        
        # Get start and end date from Github env.
        self.start = '2020-01-01' #os.getenv('start_date')
        self.end = '2023-01-01'#os.getenv('end_date')

        # Get trading-adjusted date of start and end.
        self.start_adj = para.exc.annd_adjusted(self.start)
        self.end_adj = para.exc.annd_adjusted(self.end)

        # Test columns for fundamentals ingest.
        self.ingest_column = os.getenv('ingest_column')
    
    def test_get_universe(self):
        assert len(self.stock_univers) > 1

    def test_get_history_data(self):
        # r834, r405, r27
        data = get_history_data(ticker=self.stock_univers, 
                                           columns=['psr_tej','vol_dtp', 'shrp_o1000', 'idx_mcap', 'opi', 'Security_Name', 'Security_Full_Name'],
                                           transfer_to_chinese = False,
                                           start= self.start,
                                           end = self.end,
                                           require_annd = True)

        assert_date(data, self.start_adj, self.end_adj)

    def test_all_columns(self):
        data = get_history_data(ticker=self.stock_univers, 
                                    columns=self.all_columns,
                                    transfer_to_chinese = False,
                                    start= self.end - pd.Timedelta(180),
                                    )

        assert_date(data, self.start_adj, self.end_adj)

    def test_big_data(self):
        '''
        Test data retreiving function from 2000-01-01 till now.
        '''
        test_columns = []
        for i in para.table_columns['TABLE_NAMES'].unique():
            test_columns += para.table_columns.query(f"TABLE_NAMES == '{i}'")['COLUMNS'].tail(5).tolist()

        data = get_history_data(ticker=self.stock_univers, 
                                    columns=test_columns,
                                      transfer_to_chinese = True,
                                        fin_type=['A','Q'],
                                        include_self_acc='Y', 
                                        start= '2000-01-01')

        assert_date(data, self.start_adj, self.end_adj)

    def test_ingest_fundamentals(self):
        os.environ['tickers'] = ' '.join(self.stock_univers)
        os.environ['mdate'] = self.start+' '+self.end
        os.environ['fields'] = self.ingest_column
        exec('!zipline ingest -b fundamentals')

    def test_diff_tool_ingest(self):
        tool_data = get_history_data(ticker=self.stock_univers, 
                                    columns=self.ingest_column,
                                    transfer_to_chinese = False,
                                    start= self.start)
        ingest_data = get_fundamentals(fields=['symbol', 'date', 'fin_date'] + self.ingest_column,
                                        frequency='Daily')
        
        tool_data = tool_data.merge(ingest_data, left_on = ['coid', 'mdate'], right_on = ['symbol', 'date'], suffixies = ('','_ingest'))

        result = []
        for col in self.ingest_column:
            diff = tool_data[col] - tool_data[col]
            result.append(diff.sum())

        assert result.sum() == 0
    
