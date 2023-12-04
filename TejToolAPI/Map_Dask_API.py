import tejapi
import pandas as pd
import datetime
import numpy as np
import dask.dataframe as dd
import dask
import gc
from . import parameters as para
from .meta_types import (
    # Fin_meta_types,
    # Stk_meta_types,
    # Alt_Event_meta_types,
    Meta_Types
                         )

def get_fin_data(table, tickers, columns=[], **kwargs):
    start = kwargs.get('start', para.default_start)
    end = kwargs.get('end', para.default_end)
    fin_type = kwargs.get('fin_type', ['A', 'Q', 'TTM'])
    # transfer_to_chinese = False
    npartitions = kwargs.get('npartitions', para.npartitions_local)
    
    # 將需要的 column 選出
    columns += ['coid', 'mdate', 'annd', 'no', 'key3']
    # columns += ['coid', 'mdate', 'annd', 'key3']
    columns = list(set(columns))

    # get all data
    def get_data(table, tickers, columns, start, end, fin_type): 
        data_sets = tejapi.fastget(table,
                        coid=tickers,
                        key3 = fin_type,
                        paginate=True,
                        chinese_column_name=False,
                        mdate={'gte': start, 'lte': end},
                        opts={'columns': columns, 'sort':{'coid.asc', 'mdate.asc', 'annd.asc', 'no.asc'}})
        # 
        new_columns = []
        for c in columns:
            if c in ['coid', 'mdate','key3','no','annd', 'sem','fin_type', 'curr', 'fin_ind']:
            # if c in ['coid', 'mdate','key3','annd', 'sem','fin_type', 'curr', 'fin_ind']:
                new_columns.append(c)
            else:
                for ft in fin_type:
                    new_columns.append(c+'_'+ft)
        
        new_columns.remove('key3')
        fix_col_dict = {i:pd.Series(dtype=Meta_Types.all_meta[i]) for i in new_columns}
        fix_col_df = pd.DataFrame(fix_col_dict)

        if len(data_sets) < 1:

            return fix_col_df
        temp = data_sets[['coid', 'key3','annd', 'mdate', 'no']].copy()
        fin_date = getMRAnnd_np(temp)
        data_sets = fin_date.merge(data_sets, how = 'left', on = ['coid', 'key3','annd', 'mdate', 'no'])
        # if 'ver' in data_sets.columns:
        #     data_sets.drop(columns = 'ver')

        # parallel fin_type to columns 
        keys = ['coid', 'mdate', 'no', 'annd'] + [c  for c in columns if c in ['sem','fin_type', 'curr', 'fin_ind']]
        data_sets = fin_pivot(data_sets, remain_keys=keys)

        # Ensure there is no difference between data_sets and alt_dfs.
        col_diff = set(data_sets.columns).difference(set(fix_col_df.columns))

        # If difference occurred, update alt_dfs.
        if len(col_diff)>0:
            fix_col_dict.update({i:data_sets[i].dtypes.name for i in list(col_diff)})
            fix_col_df = pd.DataFrame(fix_col_dict)

        # Fixed the order of the columns
        data_sets = data_sets[fix_col_df.columns]
        data_sets = parallize_annd_process(data_sets)

        return data_sets
    
    # Define the meta of the dataframe
    # meta = get_data(table = table, tickers = '2330', columns = columns, start = start, end =end, fin_type= fin_type)
    # meta = Fin_meta_types.meta_types

    # Calculate the number of tickers in each partition.
    ticker_partitions = get_partition_group(tickers=tickers, npartitions=npartitions)
    
    # Submit jobs to the parallel cores
    multi_subsets = [dask.delayed(get_data)(table=table, tickers = tickers[(i-1)*npartitions:i*npartitions], columns = columns, start = start, end = end, fin_type = fin_type) for i in range(1, ticker_partitions)]
    data_sets = dd.from_delayed(multi_subsets)
    data_sets = data_sets.drop_duplicates(subset=['coid', 'annd'], keep = 'last')
    
    # If ticker smaller than defaulted partitions, then transform it into defaulted partitions
    if data_sets.npartitions < npartitions:
        data_sets = data_sets.repartition(npartitions=npartitions)
    
    # if '2330' not in tickers:
    #     data_sets = data_sets.loc[~(data_sets['coid']=='2330'),:]
    
    return data_sets


def get_trading_data(table, tickers, columns = [], **kwargs):
    # Setting default value of the corresponding parameters
    start = kwargs.get('start', para.default_start)
    end = kwargs.get('end', para.default_end)
    npartitions = kwargs.get('npartitions',  para.npartitions_local)

    # 自動補上 coid, mdate
    columns += ['coid', 'mdate']
    columns = list(set(columns))

    def _get_data(table, tickers, columns, start, end):
        data = tejapi.fastget(table,
                        coid = tickers,
                        paginate = True,
                        chinese_column_name=False,
                        mdate = {'gte':start,'lte':end},
                        opts = {'columns':columns, 'sort':{'coid.asc', 'mdate.asc'}})
        # 
        fix_col_dict = {i:pd.Series(dtype=Meta_Types.all_meta[i]) for i in columns}
        fix_col_df = pd.DataFrame(fix_col_dict)

        if len(data) < 1:

            return fix_col_df
        
        # Ensure there is no difference between data and alt_dfs.
        col_diff = set(data.columns).difference(set(fix_col_df.columns))

        # If difference occurred, update alt_dfs.
        if len(col_diff)>0:
            fix_col_dict.update({i:data[i].dtypes.name for i in list(col_diff)})
            fix_col_df = pd.DataFrame(fix_col_dict)

        # Fixed the order of the columns
        data = data[fix_col_df.columns]
        
        return data
    
    # Define the meta of the dataframe
    # meta = _get_data(table = table, tickers = '2330', columns=columns, start = start, end =end)
    # meta = {Stk_meta_types.meta_types[i] for i in columns}

    # Calculate the number of tickers in each partition.
    ticker_partitions = get_partition_group(tickers=tickers, npartitions=npartitions)
    
    # Submit jobs to the parallel cores
    multi_subsets = [dask.delayed(_get_data)(table, tickers[(i-1)*npartitions:i*npartitions], columns, start, end) for i in range(1, ticker_partitions)]
    data_sets = dd.from_delayed(multi_subsets)
    data_sets = data_sets.drop_duplicates(subset=['coid', 'mdate'], keep = 'last')

    # If ticker smaller than defaulted partitions, then transform it into defaulted partitions
    if data_sets.npartitions < npartitions:
        data_sets = data_sets.repartition(npartitions=npartitions)

    return data_sets

def get_alternative_data(table, tickers=[], columns = [], **kwargs):
    start = kwargs.get('start', para.default_start)
    end = kwargs.get('end', para.default_end)
    # transfer_to_chinese = False
    npartitions = kwargs.get('npartitions', para.npartitions_local)

    # get column of event date 
    annd = get_event_column(table)
    # Fill coid, mdate and announce date(annd) to columns
    columns += ['coid','mdate', annd]
    # If table contains 'key3', add key3 to columns
    if 'key3' in tejapi.table_info(table)['columns'].keys():
        columns += ['key3']

    columns = list(set(columns))


    def _get_data(table, tickers, columns, start, end):
        # 營業日
        days = para.exc.calendar
        days = days.rename(columns = {'zdate':'all_dates'})
        # get column of event date 
        annd = get_event_column(table)
        
        # alternative data
        data_sets = tejapi.fastget(table,
                        coid = tickers,
                        paginate = True,
                        chinese_column_name=False,
                        mdate = {'gte':start,'lte':end},
                        opts = {'columns':columns, 'sort':{'coid.asc', 'mdate.asc', f'{annd}.asc'}})
        
        # Create the empty dataframe with columns and the corresponding types.
        new_columns = []
        for i in columns:
            if i == annd:
                new_columns.append(get_repo_column(table))

            elif i == 'key3' and rename_alt_event_key3(table)!='':
                new_columns.append(rename_alt_event_key3(table))

            else:
                new_columns.append(i)
        
        alt_df = ({'all_dates': pd.Series(dtype=Meta_Types.all_meta['all_dates'])})
        alt_df.update({i:pd.Series(dtype=Meta_Types.all_meta[i]) for i in new_columns})
        alt_dfs = pd.DataFrame(alt_df)

        if len(data_sets) < 1:    
            return alt_dfs
        
        data_sets = parallize_annd_process(data_sets, annd= annd)

        # # 創建一個向量化的函數
        # vectorized_annd_adjusted = np.vectorize(annd_adjusted)

        # # 所有不同的發布日
        # uni_dates = pd.to_datetime(data_sets[annd].dropna().unique())
        # # print(uni_dates)

        # # 傳入 ExchangeCalendar 物件
        # result = vectorized_annd_adjusted(para.exc, uni_dates)

        # # Create a mapping dictionary
        # dict_map = {uni_dates[i]:result[i] for i in range(len(result))}

        # # Adjust non-trading announce date to next trading date.
        # data_sets[annd] = data_sets[annd].map(dict_map)

        data_sets = dd.merge(days, data_sets, how='inner', left_on = ['all_dates'], right_on=[annd])
        

        if annd!='mdate':
            data_sets = data_sets.rename(columns = {annd:get_repo_column(table)})

        if rename_alt_event_key3(table)!='':
            data_sets = data_sets.rename(columns={'key3':rename_alt_event_key3(table)})

        # Ensure there is no difference between data_sets and alt_dfs.
        col_diff = set(data_sets.columns).difference(set(alt_dfs.columns))

        # If difference occurred, update alt_dfs.
        if len(col_diff)>0:
            alt_df.update({i:data_sets[i].dtypes.name for i in list(col_diff)})
            alt_dfs = pd.DataFrame(alt_df)

        # Fix the order of columns
        data_sets = data_sets[alt_dfs.columns]
        

        return data_sets

    # Define the meta of the dataframe
    # meta = Alt_Event_meta_types.meta_types

    # Calculate the number of tickers in each partition.
    ticker_partitions = get_partition_group(tickers=tickers, npartitions=npartitions)
    
    # Submit jobs to the parallel cores
    multi_subsets = [dask.delayed(_get_data)(table, tickers[(i-1)*npartitions:i*npartitions], columns, start, end) for i in range(1, ticker_partitions)]
    data_sets = dd.from_delayed(multi_subsets)
    # data_sets = data_sets.drop_duplicates(subset=['coid', get_repo_column(table), annd], keep = 'last')
    data_sets = data_sets.drop_duplicates(subset=['coid', get_repo_column(table)], keep = 'last')

    # If ticker smaller than defaulted partitions, then transform it into defaulted partitions
    if data_sets.npartitions < npartitions:
        data_sets = data_sets.repartition(npartitions=npartitions)

    # data_sets = data_sets.groupby('coid', group_keys = False).apply(TejToolAPI.Map_Dask_API.fillna_multicolumns)

    return data_sets


def get_fin_auditor(table, tickers, columns=[], **kwargs):
    # Setting defualt value of the parameters
    start = kwargs.get('start', para.default_start)
    # print(start)
    end = kwargs.get('end', para.default_end)
    fin_type = kwargs.get('fin_type', ['A', 'Q', 'TTM'])
    # transfer_to_chinese = False
    npartitions = kwargs.get('npartitions', para.npartitions_local)

    # 自動補上 coid, mdate
    columns += ['coid', 'mdate','key3','annd', 'no']
    columns = list(set(columns))

    # get fin data
    def _get_data(table, tickers, columns, start, end, fin_type):
        data_sets = tejapi.fastget(table,
                            coid = tickers,
                            key3 = fin_type, 
                            paginate = True,
                            chinese_column_name=False,
                            mdate = {'gte':start,'lte':end},
                            opts= {'columns':columns, 'sort':{'coid.asc', 'mdate.asc', 'no.asc','key3.asc', 'annd.asc'}})
        
        new_columns = []
        for c in columns:
            if c in ['coid', 'mdate','key3','no','annd', 'sem', 'fin_type', 'curr', 'fin_ind']:
                new_columns.append(c)
            else:
                for ft in fin_type:
                    new_columns.append(c+'_'+ft)
        
        new_columns.remove('key3')
        lower_new_columns = {i:i.lower() for i in new_columns}
        fix_col_dict = {i:pd.Series(dtype=Meta_Types.all_meta[i]) for i in lower_new_columns}
        fix_col_df = pd.DataFrame(fix_col_dict)

        if len(data_sets) < 1:
            return fix_col_df
        
        # modify the name of the columns from upper case to lower case.
        lower_columns = {i:i.lower() for i in data_sets.columns}
        data_sets = data_sets.rename(columns=lower_columns)

        # get most recent announce date of the company
        temp = data_sets[['coid', 'key3','annd', 'mdate', 'no']].copy()
        fin_date = getMRAnnd_np(temp)
        data_sets = fin_date.merge(data_sets, how = 'left', on = ['coid', 'key3','annd', 'mdate', 'no'])

        # del fin_date
        # gc.collect()
        
        # Select columns
        try:
            data_sets = data_sets.loc[:,columns]
        
        except:
            # Check selected columns exist in the columns of dataframe
            selected_columns = [i for i in columns if i in data_sets.columns]
            
            # Ensure `data_sets` does not have duplicate rows.
            selected_columns = list(set(selected_columns))
            
            # Select columns again
            data_sets = data_sets.loc[:,selected_columns]

        # Parallel fin_type (key3) to columns
        fin_keys = ['coid', 'mdate', 'annd', 'no', 'sem', 'merg', 'curr', 'fin_ind']
        keys = [i for i in columns if i in fin_keys]
        data_sets = fin_pivot(data_sets, remain_keys=keys)

        # Ensure there is no difference between data_sets and alt_dfs.
        col_diff = set(data_sets.columns).difference(set(fix_col_df.columns))

        # If difference occurred, update alt_dfs.
        if len(col_diff)>0:
            fix_col_dict.update({i:data_sets[i].dtypes.name for i in list(col_diff)})
            fix_col_df = pd.DataFrame(fix_col_dict)

        # Fixed the order of the columns
        data_sets = data_sets[fix_col_df.columns]
        data_sets = parallize_annd_process(data_sets)
        
        return data_sets

    # Define the meta of the dataframe
    # meta = _get_data(table = table, tickers = '2330', columns = columns, start = start, end =end, fin_type= fin_type)
    # meta = Fin_meta_types.meta_types

    # 
    ticker_partitions = get_partition_group(tickers=tickers, npartitions=npartitions)
    
    # Submit jobs to the parallel cores
    multi_subsets = [dask.delayed(_get_data)(table=table, tickers = tickers[(i-1)*npartitions:i*npartitions], columns = columns, start = start, end = end, fin_type = fin_type) for i in range(1, ticker_partitions)]
    data_sets = dd.from_delayed(multi_subsets)
    data_sets = data_sets.drop_duplicates(subset=['coid','annd'], keep = 'last')        
    
    # If ticker smaller than defaulted partitions, then transform it into defaulted partitions
    if data_sets.npartitions < npartitions:
        data_sets = data_sets.repartition(npartitions=npartitions)

    return data_sets

# def getMRAnnd(data):
#     # Keep the last row when there are multiple rows with the same keys
#     # The last row represents the data with the greatest mdate
#     # data = data.drop_duplicates(subset=['coid', 'key3','annd'], keep='last')
#     data_dd = data.drop_duplicates(subset=['coid', 'mdate', 'key3', 'annd'], keep = 'last')
#     data_dd = data_dd.sort_values(['coid', 'mdate', 'key3', 'annd'])
#     data = data_dd.drop_duplicates(subset=['coid', 'mdate', 'key3'], keep = 'first') 
#     check_no = data.groupby(['coid', 'mdate', 'key3'])['annd'].count().reset_index()
#     print(sum(check_no['annd']>1) == 0)
    
#     del data_dd, check_no
#     gc.collect

#     return data

def getMRAnnd_np(data):
    # data = parallize_annd_process(data)
    # Keep the last row when there are multiple rows with the same keys
    # The last row represents the data with the greatest mdate
    # data = data.drop_duplicates(subset=['coid', 'key3','annd'], keep='last')
    data['ver'] = data['mdate'].astype(str) + '-' + data['no']
    data = data.groupby(['coid', 'key3','annd'], as_index=False).max()
    data = data.groupby(['coid','key3']).apply(lambda x: np.fmax.accumulate(x, axis=0))
    data = parallelize_ver_process(data)
    data = data.drop(columns = 'ver')

    return data

def get_announce_date(tickers, **kwargs):
    start = kwargs.get('start', para.default_start)
    end = kwargs.get('end', para.default_end)
    fin_type = kwargs.get('fin_type', ['A', 'Q', 'TTM'])

    data = tejapi.fastget('TWN/AINVFINQA',
                    coid = tickers,
                    paginate = True,
                    key3 = fin_type,
                    chinese_column_name=False, 
                    mdate = {'gte':start, 'lte':end},
                    opts = {'sort':{'coid.asc', 'mdate.asc', 'key3.asc', 'no.asc','annd.asc'}})

    
    data = parallize_annd_process(data)
    # Keep the last row when there are multiple rows with the same keys
    # The last row represents the data with the greatest mdate
    # data = data.drop_duplicates(subset=['coid', 'key3','annd'], keep='last')
    data['ver'] = data['mdate'].astype(str) + '-' + data['no']
    data = data.groupby(['coid', 'key3','annd'], as_index=False).max()
    data = data.groupby(['coid','key3']).apply(lambda x: np.fmax.accumulate(x, axis=0))
    data = parallelize_ver_process(data)
    data.drop(columns = 'ver')

    return data

def parallize_annd_process(data, annd = 'annd'):
    # annd
    # 創建一個向量化的函數
    vectorized_annd_adjusted = np.vectorize(annd_adjusted)

    # 所有不同的發布日
    uni_dates = pd.to_datetime(data[annd].dropna().unique())
    # print(uni_dates)

    # 傳入 ExchangeCalendar 物件
    result = vectorized_annd_adjusted(para.exc, uni_dates)

    # Create a mapping dictionary
    dict_map = {uni_dates[i]:result[i] for i in range(len(result))}

    data[annd] = data[annd].map(dict_map)
    # print(data[annd])
    
    return data

def parallelize_ver_process(data):
    def parallel_process(func, data):
        vectorized = np.vectorize(func)
        uni_dates = data['ver'].unique()
        result = vectorized(uni_dates)
        dict_map = {uni_dates[i]:result[i] for i in range(len(result))}
        return dict_map
    
    # mdate
    mdate_dict = parallel_process(split_mdate, data)
    data['mdate'] = data['ver'].map(mdate_dict)

    # no
    no_dict = parallel_process(split_no, data)
    data['no'] = data['ver'].map(no_dict)

    return data


# def annd_adjusted(ExchangeCalendar, date):
#         if ExchangeCalendar.is_session(date):
#             return date
        
#         return ExchangeCalendar.next_open(date)

def annd_adjusted(ExchangeCalendar, date, shift_backward=True):
        if ExchangeCalendar.is_session(date):
            return date
        
        if shift_backward:
            return ExchangeCalendar.prev_open(date)
        
        return ExchangeCalendar.next_open(date)

def split_mdate(string:str):
    mdate = string.split('-')[:2]
    mdate = pd.to_datetime('-'.join(mdate))
    return mdate

def split_no(string:str):
    no = string.split('-')[-1]
    return no

def fin_pivot(df, remain_keys):
    # for loop execute pviot function
    uni = df['key3'].dropna().unique()
    data = pivot(df, remain_keys, uni[0])

    for i in range(1, len(uni)):
        temp = pivot(df, remain_keys, uni[i])
        data = data.merge(temp, on = remain_keys)

    return data


def pivot(df, remain_keys, pattern):
    try:
        data = df.loc[df['key3']==pattern, :]
        # Create a mapping table of column names and their corresponding new names.
        new_keys = {i:i+'_'+str(pattern) for i in data.columns.difference(remain_keys)}
        
        # Replace old names with the new ones.
        data = data.rename(columns = new_keys)
        data = data.loc[:,~data.columns.str.contains('key3')]
    
    except:
        raise ValueError('請使用 get_announce_date 檢查該檔股票的財務數據發布日是否為空值。')
    
    return data


def generate_multicalendars(tickers, **kwargs):
    # Setting defualt value of the parameters
    start = kwargs.get('start', para.default_start)
    end = kwargs.get('end', para.default_end)
    npartitions = kwargs.get('npartitions', para.npartitions_local)

    def get_daily_calendar(tickers:list):
        def get_data(ticker):
            cal = pd.date_range(start=start, end=end, freq='D')
            coid = [str(ticker)]*len(cal)
            return pd.DataFrame({'coid':coid, 'all_dates': cal})
        
        # Multi-ticker method
        if len(tickers) > 1:
            data_sets = pd.DataFrame()
            for ticker in tickers:
                data = get_data(ticker = ticker)
                data_sets = pd.concat([data_sets, data])
                
        # Single ticker mehtod
        else:
            data_sets = get_data(ticker = tickers)

        return data_sets

    # Define the meta for the dataframe
    meta = pd.DataFrame({'coid': pd.Series(dtype='object'), 'all_dates': pd.Series(dtype='datetime64[ns]')})
    
    # Calculate the number of tickers in each partition. 
    ticker_partitions = get_partition_group(tickers=tickers, npartitions=npartitions)
    
    # Submit jobs to the parallel cores
    fin_calendar = dd.from_delayed([dask.delayed(get_daily_calendar)(tickers[(i-1)*npartitions:i*npartitions]) for i in range(1, ticker_partitions)], meta = meta)
    
    # If ticker smaller than defaulted partitions, then transform it into defaulted partitions
    if fin_calendar.npartitions < para.npartitions_local:
        fin_calendar = fin_calendar.repartition(npartitions=npartitions)

    return fin_calendar

# def generate_multicalendars_dask(tickers, **kwargs):
#     # Setting defualt value of the parameters
#     start = kwargs.get('start', para.default_start)
#     end = kwargs.get('end', para.default_end)
#     npartitions = kwargs.get('npartitions', para.npartitions_local)


#     def get_daily_calendar(tickers:list):
#         def get_data(ticker):
#             cal = pd.date_range(start=start, end=end, freq='D')
#             coid = [str(ticker)]*len(cal)
#             return pd.DataFrame({'coid':coid, 'all_dates': cal})
        
#         # Multi-ticker method
#         if len(tickers) > 1:
#             data_sets = pd.DataFrame()
#             for ticker in tickers:
#                 data = get_data(ticker = ticker)
#                 data_sets = pd.concat([data_sets, data])
                
#         # Single ticker mehtod
#         else:
#             data_sets = get_data(ticker = tickers)

#         return data_sets
    
#     # Define the meta for the dataframe
#     meta = pd.DataFrame({'coid': pd.Series(dtype='object'), 'all_dates': pd.Series(dtype='datetime64[ns]')})
    
#     # Calculate the number of tickers in each partition. 
#     ticker_partitions = get_partition_group(tickers=tickers, npartitions=npartitions)

#     # Submit jobs to the parallel cores
#     fin_calendar = dd.from_delayed([dask.delayed(get_daily_calendar)(tickers[(i-1)*npartitions:i*npartitions]) for i in range(1, ticker_partitions)])
    
#     # If ticker smaller than defaulted partitions, then transform it into defaulted partitions
#     if fin_calendar.npartitions < npartitions:
#         fin_calendar = fin_calendar.repartition(npartitions=npartitions)

#     return fin_calendar

def get_most_recent_date(data, sort_keys, subset, keep_mothod):
    # sort data order by mdate(accural date) and annd_s(announce date)
    data = data.sort_values(sort_keys)

    # when multiple rows have the same annd_s(announce date), keep the last row, which has the greatest mdate.
    data = data.drop_duplicates(subset = subset, keep = keep_mothod)

    return data


def fillna_multicolumns(df):
    return df.fillna(method = 'ffill')

def get_partition_group(tickers:list, npartitions):
    return (len(tickers)//npartitions) + 1 if len(tickers)%npartitions ==0 else (len(tickers)//npartitions) + 2

def get_event_column(table):
    return para.event_column.loc[para.event_column['API_TABLE'].isin([table]), 'EVENT_COLUMN'].item()

def get_repo_column(table):
    return para.event_column.loc[para.event_column['API_TABLE'].isin([table]), 'REPO_COLUMN'].item()

def rename_alt_event_key3(table):
    if not para.event_column.loc[para.event_column['API_TABLE'].isin([table]), 'KEY3'].isna().item():
        return para.event_column.loc[para.event_column['API_TABLE'].isin([table]), 'KEY3'].item()
    else:
        return ''


def get_auditor_acc(table, tickers, columns, start, end, fin_type):
    # try:
    columns.remove('annd')
    data_sets = tejapi.fastget(table,
                        coid = tickers,
                        key3 = fin_type, 
                        paginate = True,
                        chinese_column_name=False,
                        mdate = {'gte':start,'lte':end},
                        opts= {'columns':columns, 'sort':{'coid.asc', 'mdate.asc', 'key3.asc', 'no.asc'}, 'pivot':True})
    
    columns.append('annd')
    new_columns = []
    for c in columns:
        if c in ['coid', 'mdate','key3','no','annd', 'sem','fin_type', 'curr', 'fin_ind']:
            new_columns.append(c)
        else:
            for ft in fin_type:
                new_columns.append(c+'_'+ft)
    
    new_columns.remove('key3')
    lower_new_columns = {i:i.lower() for i in new_columns}
    fix_col_dict = {i:pd.Series(dtype=Meta_Types.all_meta[i]) for i in lower_new_columns}
    fix_col_df = pd.DataFrame(fix_col_dict)

    if len(data_sets) < 1:
        return fix_col_df
    
    # modify the name of the columns from upper case to lower case.
    lower_columns = {i:i.lower() for i in data_sets.columns}
    data_sets = data_sets.rename(columns=lower_columns)

    # get most recent announce date of the company
    fin_date = get_announce_date(tickers, start = start, end = end, fin_type = fin_type)
    data_sets = fin_date.merge(data_sets, how = 'left', on = ['coid', 'mdate', 'key3', 'no'])

    del fin_date
    gc.collect()
    
    # Select columns
    try:
        data_sets = data_sets.loc[:,columns]
    
    except:
        # Check selected columns exist in the columns of dataframe
        selected_columns = [i for i in columns if i in data_sets.columns]
        
        # Ensure `data_sets` does not have duplicate rows.
        selected_columns = list(set(selected_columns))
        
        # Select columns again
        data_sets = data_sets.loc[:,selected_columns]

    # Parallel fin_type (key3) to columns
    keys = ['coid', 'mdate', 'no', 'annd'] #+ [c  for c in columns if c in ['sem','fin_type', 'curr', 'fin_ind']]
    data_sets = fin_pivot(data_sets, remain_keys=keys)

    # Ensure there is no difference between data_sets and alt_dfs.
    col_diff = set(data_sets.columns).difference(set(fix_col_df.columns))

    # If difference occurred, update alt_dfs.
    if len(col_diff)>0:
        fix_col_dict.update({i:data_sets[i].dtypes.name for i in list(col_diff)})
        fix_col_df = pd.DataFrame(fix_col_dict)

    # Fixed the order of the columns
    data_sets = data_sets[fix_col_df.columns]
        
    return data_sets


class TejDaskAPI:
    def __init__(self, 
                 table,
                 ticker,
                 columns,
                 start= para.default_start,
                 end = para.default_end,
                 fin_type = ['A', 'Q', 'TTM'],
                 npartitions = para.npartitions_local,
                 **kwargs):
        self.table = table
        self.ticker = ticker
        self.columns = columns
        self.start = start
        self.end = end
        self.fin_type = fin_type
        self.npartitions = npartitions

    @property
    def get_fin_data(self, table, ticker, columns, start, end , fin_type, npartitions):
        self.columns += ['coid', 'mdate', 'annd', 'no', 'key3']
        self.columns = list(set(self.columns))
        # def get_data(self):
        datasets = tejapi.fastget(self.table,
                    coid = self.ticker,
                    mdate = {'gte':self.start, 'lte':self.end},
                    key3 = self.fin_type,
                    paginate=True,
                    chinese_column_name=False,
                    opts={'columns': self.columns, 'sort':{'coid.asc', 'mdate.asc', 'annd.asc', 'no.asc'}} 
                    )
        
        return datasets
        