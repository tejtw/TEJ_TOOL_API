import tejapi
import dask
import dask.dataframe as dd
import pandas as pd
import gc
from . import parameters as para
from .Map_Dask_API import ToolApiMeta , FinSelfAccData , TradingData , AlternativeData , FinAuditorData
from .utils import get_api_key_info 
from .meta_types import Meta_Types 
import re

global pandas_main_version , all_meta
pandas_main_version = pd.__version__.split('.')[0]
all_meta = Meta_Types.all_meta
dask.config.set({'dataframe.convert-string': False})

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


    columns = get_internal_code(columns)

    trading_data_dict = TradingData(
            tickers=ticker ,
            start= shift_start ,
            end = end ,
            extend_fg = 'N' ,
            npartitions=npartitions , 
            columns = columns,
            freq = fin_type 
            ).run()
    alternative_data_dict = AlternativeData(
            tickers=ticker ,
            start= shift_start ,
            end = end ,
            extend_fg = 'N' ,
            npartitions=npartitions , 
            columns = columns,
            freq = fin_type 
            ).run()
    fin_audit_data_dict = FinAuditorData(
            tickers=ticker ,
            start= shift_start ,
            end = end ,
            extend_fg = 'N' ,
            npartitions=npartitions , 
            columns = columns,
            freq = fin_type 
            ).run()
    if include_self_acc == 'Y' :
        fin_self_acc_data_dict = FinSelfAccData(
            tickers=ticker ,
            start= shift_start ,
            end = end ,
            extend_fg = 'N' ,
            npartitions=npartitions , 
            columns = columns,
            freq = fin_type 
            ).run()
        fin_audit_data_dict.update(fin_self_acc_data_dict)
    
    for key , value in fin_audit_data_dict.items() :
        fin_audit_data_dict[key] = process_fin_data(value)

    all_dict = trading_data_dict.copy()
    all_dict.update(alternative_data_dict)
    all_dict.update(fin_audit_data_dict)
    # Consecutive merge.
    
    data = consecutive_merge(tables= all_dict ,
                             tickers=ticker, 
                             start=shift_start, 
                             end=end,
                             npartitions=npartitions,
                             )
    data = data.compute(meta = all_meta)
    
    # # Drop redundant columns of the merged table.
    if require_annd:
        data = data.drop(columns=[i for i in data.columns if i in para.drop_keys])
        column_prefix = ['coid' , 'mdate' ,'fin_date', 'mon_sales_date', 'share_date']
    else:
        data = data.drop(columns=[i for i in data.columns if i in para.drop_keys+['fin_date', 'mon_sales_date', 'share_date']])
        column_prefix = ['coid' , 'mdate']

    triggered_columns = search_table(columns)

    fill_dict = dict(zip(triggered_columns['COLUMNS'], triggered_columns['fill_fg']))

    for column in data.columns :
        for valid_column in triggered_columns['COLUMNS'].tolist() :
            if re.match(valid_column,column) :
                column_prefix.append(column)
                value = fill_dict[valid_column]
                fill_dict[column] = value
                break

    status_column = [column for column in column_prefix if ( column in data.columns and (column in column_prefix or fill_dict.get(column) == 'Y') )  ]

    n_status_column = [column for column in column_prefix if ( column in data.columns and (column in column_prefix or fill_dict.get(column) != 'Y') )  ]
    
    event_data = data.loc[: , n_status_column]

    data = data.loc[: , status_column]
    
    column_prefix = list(set(column_prefix).intersection(set(data.columns.tolist())))
    # Drop repeat rows from the table.

    # Sort values by coid and mdate
    data = data.sort_values(['coid', 'mdate'] , ascending=True).reset_index(drop=True)
    
    # Apply forward value to fill the precending NaN.
    
    data = data.groupby(['coid'], group_keys=False).apply(lambda x: x.ffill())
    
    # merge back evnet-type data, which does not need ffill.
    data = dd.merge(data, event_data, on= column_prefix , how = 'left')

    data = data.compute(meta = all_meta )

    # Drop suspend trading day
    coid_calendar = get_stock_calendar(tickers = ticker, start = org_start , end = end , npartitions = npartitions).compute()
    
    data = dd.merge(coid_calendar, data, on= ['coid','mdate'], how = 'left')
    data = data.compute(meta = all_meta)

    # Truncate resuly by user-setted start.
    data = data.loc[data.mdate >= pd.Timestamp(org_start),:]

    # Transfer columns to abbreviation text.
    
    lang_map = transfer_language_columns(data.columns, isChinese=transfer_to_chinese)
    data = data.rename(columns= lang_map)
    data = data.reset_index(drop=True)
    data = data.sort_values(by = ['coid','mdate'])
    get_api_key_info(show_progress)

    return data

def process_fin_data(table):
    # transfer to daily basis
    days = para.exc.calendar
    days = days.rename(columns = {'zdate':'all_dates'})
    if (pandas_main_version != '1') :
        table['annd'] = table['annd'].astype('datetime64[ms]')
        table['mdate'] = table['mdate'].astype('datetime64[ms]')
    else :
        table['annd'] = table['annd'].astype('datetime64[ns]')
        table['mdate'] = table['mdate'].astype('datetime64[ns]')

    table = table.rename(columns = {'mdate':'fin_date'})
    table = dd.merge(days, table, left_on=['all_dates'], right_on=['annd'], how='left')

    # Delete the redundant dataframe to release memory space
    del days
    gc.collect()
    
    return table


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
        # Remove  _A, _Q, _TTM , \d+
        if ( re.search('_A$|_Q$|_TTM$|_\d+$' , col) ) :
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

def extend_columns(data : dd.DataFrame) :
    """
    依據 fin_invest_tables 中的 EXTEND_FG 進行欄位擴增
    """
    column_names = data.columns.tolist()
    x = data.groupby(['coid' ,'mdate','all_dates']).agg(list).compute().reset_index()
    
    data_list = [x[['coid','mdate','all_dates']]]
    for column in column_names :
        if column in ['coid','mdate','all_dates'] :
            continue
        temp = x[column].apply(pd.Series)
        temp.columns = temp.columns+1
        temp = temp.add_prefix(f'{column}_')
        data_list.append(temp)

    result = dd.concat(data_list , axis =1)
    

    return result

def get_internal_code(fields:list):
    columns = []
    for c in ['ENG_COLUMN_NAMES', 'CHN_COLUMN_NAMES', 'COLUMNS']:
        temp = para.transfer_language_table.loc[para.transfer_language_table[c].isin(fields), 'COLUMNS'].tolist()
        columns += temp
    columns = list(set(columns))
    return columns

def consecutive_merge(tables , **kwargs) :
    used_api_tables = [key for key in tables.keys()]
    full_para_table = para.fin_invest_tables.merge(para.map_table , on = 'TABLE_NAMES' , how = 'left')
    full_para_table = full_para_table.merge(para.merge_keys , on = 'OD' , how = 'left')
    
    trading_calendar = get_trading_calendar(kwargs.get('tickers', []),
                                            start = kwargs.get('start', para.default_start), 
                                            end = kwargs.get('end', para.default_end), 
                                            npartitions = kwargs.get('npartitions',  para.npartitions_local  )
                                            )
    for api_table in used_api_tables :
        right_merge_key = full_para_table.loc[full_para_table['API_TABLE']==api_table , 'KEYS'].tolist()
        
        trading_calendar = dd.merge(trading_calendar, tables[api_table], left_on = ['coid', 'mdate'], right_on = right_merge_key, how = 'left', suffixes = ('','_surfeit'))
        trading_calendar = trading_calendar.iloc[:,~trading_calendar.columns.str.contains('_surfeit')]
        
    
    return trading_calendar

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
                        opts = {'columns':['mdate']})
    
    def get_index_trading_date(index , tickers):

        mdate = index['mdate'].tolist()

        data = pd.DataFrame({
            'coid':[ticker for ticker in tickers for date in mdate],
            'mdate':mdate*len(tickers)
        })
        if len(data)<1:
            if pandas_main_version != '1' : 
                return pd.DataFrame({'coid': pd.Series(dtype='object'), 'mdate': pd.Series(dtype='datetime64[ms]')})
            else :
                return pd.DataFrame({'coid': pd.Series(dtype='object'), 'mdate': pd.Series(dtype='datetime64[ns]')})
        if pandas_main_version != '1' : 
            data['mdate'] = data['mdate'].astype('datetime64[ms]')
        else :
            data['mdate'] = data['mdate'].astype('datetime64[ns]')
        return data


    # Calculate the number of tickers in each partition. 
    ticker_partitions = ToolApiMeta.get_partition_group(tickers = tickers, npartitions= npartitions)

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
                        opts = {'columns':['coid', 'mdate']})
        

        if len(data)<1:
            if pandas_main_version != '1' : 
                return pd.DataFrame({'coid': pd.Series(dtype='object'), 'mdate': pd.Series(dtype='datetime64[ms]')})
            else :
                return pd.DataFrame({'coid': pd.Series(dtype='object'), 'mdate': pd.Series(dtype='datetime64[ns]')})
        if pandas_main_version != '1' :
            data['mdate'] = data['mdate'].astype('datetime64[ms]')
        else :
            data['mdate'] = data['mdate'].astype('datetime64[ns]')
        return data
            
    
    # Define the meta of the dataframe

    # Calculate the number of tickers in each partition. 
    ticker_partitions = ToolApiMeta.get_partition_group(tickers = tickers, npartitions= npartitions)

    # Submit jobs to the parallel cores
    trading_calendar = dd.from_delayed([dask.delayed(get_data)(tickers[(i-1)*npartitions:i*npartitions]) for i in range(1, ticker_partitions)])

    # If ticker smaller than defaulted partitions, then transform it into defaulted partitions
    if trading_calendar.npartitions < npartitions:
        trading_calendar = trading_calendar.repartition(npartitions=npartitions)

    return trading_calendar









