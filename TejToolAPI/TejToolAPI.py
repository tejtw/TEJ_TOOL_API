import tejapi
import dask.dataframe as dd
import pandas as pd
import gc
from . import parameters as para
from . import Map_Dask_API as dask_api
from .utils import get_api_key_info 
import dask
from .meta_types import Meta_Types 

dask.config.set({'dataframe.convert-string': False})

# 映射函數 (dask_version)
funct_map = {
    'A0001':dask_api.get_trading_data,
    'A0002':dask_api.get_fin_data,
    'A0003':dask_api.get_alternative_data,
    'A0004':dask_api.get_fin_auditor
}

def get_history_data(ticker:list, columns:list = [], fin_type:list = ['A','Q','TTM'], include_self_acc:str = 'N', **kwargs):
    """
    ticker : list , 公司碼

    columns : list , 欄位

    fin_type : list , 累計,單季,移動四季(A,Q,TTM) (optional)

    include_self_acc : ["Y","N"] , 是否包含公司自結 (optional)

    start : str , 資料起始日 (YYYY-MM-DD) (optional)

    end : str , 資料結束日 (YYYY-MM-DD) (optional)

    npartitions : int , 每組資料數量 (optional)

    require_annd : bool , 是否需要公告日欄位 (optional)

    transfer_to_chinese : bool , 欄位是否轉換為中文 (optional)

    show_progress : bool , 顯示流量狀況 (optional)
    """
    # Setting default value of the corresponding parameters
    start = kwargs.get('start', para.default_start)
    end = kwargs.get('end', para.default_end)
    transfer_to_chinese = kwargs.get('transfer_to_chinese', False)
    npartitions = kwargs.get('npartitions',  para.npartitions_local)
    require_annd = kwargs.get('require_annd', False)
    show_progress = kwargs.get('show_progress', True)

    # Shift start date 180 days backward.
    start_dt = pd.to_datetime(start)
    shift_start = start_dt - pd.Timedelta(180, 'd')
    org_start = start_dt.strftime('%Y-%m-%d')
    shift_start = shift_start.strftime('%Y-%m-%d')

    # Triggers 
    all_tables = triggers(ticker = ticker, columns= columns, start= shift_start, end= end, fin_type= fin_type, include_self_acc= include_self_acc, npartitions = npartitions)
    

    # Combind fin_self_acc and fin_auditor
    try:
        # Concate fin_self_acc with fin_auditor
        data_concat = dd.concat([all_tables['fin_self_acc'], all_tables['fin_auditor']]).reset_index(drop=True)
        all_tables['fin_auditor'] = data_concat.drop_duplicates(subset=['coid','mdate','annd'], keep='last')

        # Process two fin dataframe
        all_tables['fin_auditor'] = process_fin_data(all_tables=all_tables, variable='fin_auditor', tickers=ticker, start=start, end= end)
        
        del all_tables['fin_self_acc']

    except:
        if 'fin_auditor' in all_tables.keys():
            all_tables['fin_auditor'] = process_fin_data(all_tables=all_tables, variable='fin_auditor', tickers=ticker, start=start, end= end)

        elif 'fin_self_acc' in all_tables.keys():
            all_tables['fin_self_acc'] = process_fin_data(all_tables=all_tables, variable='fin_self_acc', tickers=ticker, start=start, end= end)
        
        else:
            pass

    # Collect variables that being triggered
    trigger_tables = [i for i in all_tables.keys() if i in para.fin_invest_tables['TABLE_NAMES'].unique().tolist()]

    # Sort by OD
    trigger_tables.sort(key = lambda x: para.map_table.loc[para.map_table['TABLE_NAMES']==x, 'OD'].item())

    # Consecutive merge.
    history_data = consecutive_merge(all_tables,  trigger_tables)



    # Drop redundant columns of the merged table.
    if require_annd:
        history_data = history_data.drop(columns=[i for i in history_data.columns if i in para.drop_keys])
        
    else:
        history_data = history_data.drop(columns=[i for i in history_data.columns if i in para.drop_keys+['fin_date', 'mon_sales_date', 'share_date']])

    # Transfer to pandas dataframe
    history_data = history_data.compute(meta = Meta_Types.all_meta)

    # Drop repeat rows from the table.
    history_data = history_data.drop_duplicates(subset=['coid', 'mdate'], keep='last')

    # Sort values by coid and mdate
    history_data = history_data.sort_values(['coid', 'mdate']).reset_index(drop=True)

    # Apply forward value to fill the precending NaN.
    history_data = history_data.groupby('coid', group_keys = False).apply(dask_api.fillna_multicolumns)
    # Drop suspend trading day
    all_tables['coid_calendar']['mdate'] = all_tables['coid_calendar']['mdate'].astype(history_data['mdate'].dtype)
    
    history_data = dd.merge(all_tables['coid_calendar'], history_data, on= ['coid','mdate'], how = 'left')
    history_data = history_data.compute(meta = Meta_Types.all_meta)

    # Truncate resuly by user-setted start.
    history_data = history_data.loc[history_data.mdate >= pd.Timestamp(org_start),:]

    # Transfer columns to abbreviation text.
    lang_map = transfer_language_columns(history_data.columns, isChinese=transfer_to_chinese)
    history_data = history_data.rename(columns= lang_map)
    history_data = history_data.reset_index(drop=True)
    get_api_key_info(show_progress)

    return history_data

def process_fin_data(all_tables, variable, tickers, start, end):
    # transfer to daily basis
    days = para.exc.calendar
    days = days.rename(columns = {'zdate':'all_dates'})
    all_tables[variable] = all_tables[variable].rename(columns = {'mdate':'fin_date'})
    all_tables[variable] = dd.merge(days, all_tables[variable], left_on=['all_dates'], right_on=['annd'], how='left')

    # Delete the redundant dataframe to release memory space
    del days
    gc.collect()
    
    return all_tables[variable]

def to_daskDataFrame(locals, indexs, npartitions=para.npartitions_local):
    for i in indexs:
        locals[i] = dd.from_pandas(locals[i], npartitions=npartitions)
    return locals

def transfer_language_columns(columns, isChinese = False):
    def get_col_name(col, isChinese):
        transfer_lang = 'CHN_COLUMN_NAMES' if isChinese else 'ENG_COLUMN_NAMES'
        try:
            col_name = search_columns([col])[transfer_lang].dropna().drop_duplicates(keep='last').item()
        except:
            col_name = search_columns([col])[transfer_lang].dropna().tail(1).item()

        return col_name if col_name else col
    
    mapping = {}
    for col in columns:
        # Remove  _A, _Q, _TTM
        check_fin_type = [col.__contains__('_A'), col.__contains__('_Q'), col.__contains__('_TTM')]
        if any(check_fin_type):
            col_stripped = col.split('_')[:-1]
            fin_type = '_' + col.split('_')[-1]
            # If the variables contain '_', then join '_' into the variables.
            if type(col_stripped) is list:
                col_stripped = '_'.join(col_stripped)

        else:
            col_stripped = col
            fin_type = ''

        # Find the corresponding Chinese column names.
        col_name = get_col_name(col_stripped, isChinese)
        if col_name not in mapping.keys():
            # 將對應關係加入 mapping
            mapping[col] = f"{col_name}{fin_type}"

    return mapping

def search_table(columns:list):
    columns = list(map(lambda x:x.lower(), columns))
    index = para.table_columns['COLUMNS'].isin(columns)
    tables = para.table_columns.loc[index, :]
    return tables

def search_columns(columns:list):
    index = para.transfer_language_table['COLUMNS'].isin(columns)
    tables = para.transfer_language_table.loc[index, :]
    return tables
def show_columns( chinese : bool = True ) :
    """
    chinese : bool , default True , else English
    """
    if chinese :
        return para.transfer_language_table['CHN_COLUMN_NAMES'].tolist()
    return  para.transfer_language_table['ENG_COLUMN_NAMES'].tolist()
def triggers(ticker:list, columns:list = [], fin_type:list = ['A','Q','TTM'],  include_self_acc:str = 'N', **kwargs):
    # Setting default value of the corresponding parameters
    start = kwargs.get('start', para.default_start)
    end = kwargs.get('end', para.default_end)
    npartitions = kwargs.get('npartitions',  para.npartitions_local)
    
    # Tranfer columns from any type (chinese, english) to internal code  
    columns = get_internal_code(columns)    

    # Kick out `coid` and `mdate` from

    columns = [col for col in columns if col not in ['coid', 'mdate','key3', 'no','sem','fin_type', 'curr', 'fin_ind'] ]

    # Qualify the table triggered by the given `columns`
    trigger_tables = search_table(columns)
    
    # Get trading calendar of all given tickers
    trading_calendar = get_trading_calendar(ticker, start = start, end = end, npartitions = npartitions)

    # If include_self_acc equals to 'N', then delete the fin_self_acc in the trigger_tables list
    if include_self_acc != 'Y':
        trigger_tables = trigger_tables.loc[trigger_tables['TABLE_NAMES']!='fin_self_acc',:]

    for table_name in trigger_tables['TABLE_NAMES'].unique():
        selected_columns = trigger_tables.loc[trigger_tables['TABLE_NAMES']==table_name, 'COLUMNS'].tolist()
        api_code = para.table_API.loc[para.table_API['TABLE_NAMES']==table_name, 'API_CODE'].item()
        api_table = para.fin_invest_tables.loc[para.fin_invest_tables['TABLE_NAMES']==table_name,'API_TABLE'].item()

        if api_code == 'A0002' or api_code == 'A0004':
            exec(f'{table_name} = funct_map[api_code](api_table, ticker, selected_columns, start = start,  end = end, fin_type = fin_type, npartitions = npartitions)')
        
        else:
            exec(f'{table_name} = funct_map[api_code](api_table, ticker, selected_columns, start = start,  end = end, npartitions = npartitions)')

    if 'stk_price' in trigger_tables['TABLE_NAMES'].unique().tolist():
        locals()['stk_price'] = locals()['stk_price'].compute()
        coid_calendar = locals()['stk_price'][['coid','mdate']]
    else:
        coid_calendar = get_stock_calendar(ticker, start = start, end = end, npartitions = npartitions).compute()
    return locals()

def get_internal_code(fields:list):
    columns = []
    for c in ['ENG_COLUMN_NAMES', 'CHN_COLUMN_NAMES', 'COLUMNS']:
        temp = para.transfer_language_table.loc[para.transfer_language_table[c].isin(fields), 'COLUMNS'].tolist()
        columns += temp
    columns = list(set(columns))
    return columns

def consecutive_merge(local_var, loop_array):
    #
    table_keys = para.map_table.merge(para.merge_keys)

    # tables 兩兩合併
    data = local_var['trading_calendar']

    for i in range(len(loop_array)):
        right_keys = table_keys.loc[table_keys['TABLE_NAMES']==loop_array[i], 'KEYS'].tolist()
        # Merge tables by dask merge.
        
        temp = local_var[loop_array[i]]
        # modified 20240226 by Han
        d = right_keys[1] # d is date
        if temp[d].dtype != data['mdate'].dtype :
            data['mdate'] = data['mdate'].astype(temp[d].dtype)
        
        data = dd.merge(data, local_var[loop_array[i]], left_on = ['coid', 'mdate'], right_on = right_keys, how = 'left', suffixes = ('','_surfeit'))
        # Drop surfeit columns.
        data = data.iloc[:,~data.columns.str.contains('_surfeit')]
    pandas_main_version = pd.__version__.split('.')[0]
    if pandas_main_version == '1' :
        data['mdate'] = data['mdate'].astype('datetime64[ns]')
    else :
        data['mdate'] = data['mdate'].astype('datetime64[ms]')
    return data

def keep_repo_date(data):
    if 'fin_date' in data.columns:
       data = data.rename({'fin_date_surfeit':'fin_date'})

    return data

def get_trading_calendar(tickers, **kwargs):
    # Setting default value of the corresponding parameters.
    start = kwargs.get('start', para.default_start)
    end = kwargs.get('end', para.default_end)
    npartitions = kwargs.get('npartitions',  para.npartitions_local)
    index = tejapi.fastget('TWN/APIPRCD',
                        coid = 'IX0001', # 台灣加權指數
                        paginate = True,
                        chinese_column_name=False,
                        mdate = {'gte':start,'lte':end},
                        opts = {'columns':['mdate'], 'sort':{'coid.asc', 'mdate.asc'}})
    
    def get_index_trading_date(index , tickers):

        mdate = index['mdate'].tolist()

        data = pd.DataFrame({
            'coid':[tick for tick in tickers for i in mdate],
            'mdate':mdate*len(tickers)
        })
        if len(data)<1:
            pandas_main_version = pd.__version__.split('.')[0]
            if pandas_main_version == '1' : 
                return pd.DataFrame({'coid': pd.Series(dtype='object'), 'mdate': pd.Series(dtype='datetime64[ns]')})
            else :
                return pd.DataFrame({'coid': pd.Series(dtype='object'), 'mdate': pd.Series(dtype='datetime64[ms]')})
    
        return data


    # Calculate the number of tickers in each partition. 
    ticker_partitions = dask_api.get_partition_group(tickers = tickers, npartitions= npartitions)

    # Submit jobs to the parallel cores
    trading_calendar = dd.from_delayed([dask.delayed(get_index_trading_date)(index , tickers[(i-1)*npartitions:i*npartitions]) for i in range(1, ticker_partitions)])

    # If ticker smaller than defaulted partitions, then transform it into defaulted partitions
    if trading_calendar.npartitions < npartitions:
        trading_calendar = trading_calendar.repartition(npartitions=npartitions)

    return trading_calendar

def get_stock_calendar(tickers, **kwargs):
    # Setting default value of the corresponding parameters.
    start = kwargs.get('start', para.default_start)
    end = kwargs.get('end', para.default_end)
    npartitions = kwargs.get('npartitions',  para.npartitions_local)

    def get_data(tickers):
        data = tejapi.fastget('TWN/APIPRCD',
                        coid = tickers, 
                        paginate = True,
                        chinese_column_name=False,
                        mdate = {'gte':start,'lte':end},
                        opts = {'columns':['coid', 'mdate'], 'sort':{'coid.asc', 'mdate.asc'}})
        

        if len(data)<1:
            pandas_main_version = pd.__version__.split('.')[0]
            if pandas_main_version == '1' : 
                return pd.DataFrame({'coid': pd.Series(dtype='object'), 'mdate': pd.Series(dtype='datetime64[ns]')})
            else :
                return pd.DataFrame({'coid': pd.Series(dtype='object'), 'mdate': pd.Series(dtype='datetime64[ms]')})
    
        return data
            
    
    # Define the meta of the dataframe

    # Calculate the number of tickers in each partition. 
    ticker_partitions = dask_api.get_partition_group(tickers = tickers, npartitions= npartitions)

    # Submit jobs to the parallel cores
    trading_calendar = dd.from_delayed([dask.delayed(get_data)(tickers[(i-1)*npartitions:i*npartitions]) for i in range(1, ticker_partitions)])

    # If ticker smaller than defaulted partitions, then transform it into defaulted partitions
    if trading_calendar.npartitions < npartitions:
        trading_calendar = trading_calendar.repartition(npartitions=npartitions)

    return trading_calendar






