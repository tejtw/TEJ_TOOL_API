import pandas as pd
import datetime
import multiprocessing as mp
import os
from .exchange_calendar import ExchangeCalendar

# Initialize exchange calendar
try:
    exc = ExchangeCalendar()
except:
    raise ValueError('請設定 TEJAPI_KEY ： os.environ["TEJAPI_KEY"] = "your_key"')
# exc = ExchangeCalendar()

# current directory
module_dir = os.path.dirname(os.path.abspath(__file__))
xlsx_path = os.path.join(module_dir,'tables','columns_group.xlsx')

# get number of cpu, and used 80% of available cores.
npartitions_local = 50 #int(mp.cpu_count()*0.8)

# set default start and end date
default_start = '2013-01-01'
default_end = datetime.datetime.now().date().strftime('%Y-%m-%d')

# drop useless keys
drop_keys = [ 'no','sem','fin_type','annd', 'annd_s','edate1','edate2','all_dates', 'fin_ind', 'curr', 'date_rmk']

# 取得每張 table 的欄位名稱(internal_code)
# get table_names, API_table, CHN_NAMES
fin_invest_tables = pd.read_excel(xlsx_path, sheet_name='fin_invest_tables')

# get table_names, columns
table_columns = pd.read_excel(xlsx_path, sheet_name='table_columns')

# get event date columns
event_column = pd.read_excel(xlsx_path, sheet_name='event_date')

# get table_names, API_code
table_API = pd.read_excel(xlsx_path, sheet_name='API')

# get chinese name and english name of the columns
transfer_language_table = pd.read_excel(xlsx_path, sheet_name='transfer_language')

# 取得 table_names, od, keys
# map_table: table_name, odd
map_table = pd.read_excel(xlsx_path,sheet_name='table_od')
# merge_keys: od, merge_keys
merge_keys = pd.read_excel(xlsx_path,sheet_name='merge_keys')





