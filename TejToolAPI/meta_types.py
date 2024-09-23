import json
import os
import pandas as pd
# current directory
module_dir = os.path.dirname(os.path.abspath(__file__))

# class Fin_meta_types:
#     xlsx_path = os.path.join(module_dir,'tables','fin_meta.json')
#     with open(xlsx_path) as json_file:
#         meta_types = json.load(json_file)
#         json_file.close()

# class Stk_meta_types:
#     xlsx_path = os.path.join(module_dir,'tables','stk_meta.json')
#     with open(xlsx_path) as json_file:
#         meta_types = json.load(json_file)
#         json_file.close()

# class Alt_Event_meta_types:
#     xlsx_path = os.path.join(module_dir,'tables','alt_event_meta.json')
#     with open(xlsx_path) as json_file:
#         meta_types = json.load(json_file)
#         json_file.close()




class Meta_Types:
    pandas_main_version = pd.__version__.split('.')[0]
    all_meta =  {
    'fin_date':'datetime64[ns]',
    'mon_sales_date':'datetime64[ns]',
    'share_date':'datetime64[ns]',
    'all_dates':'datetime64[ns]',
    'oppu_name':'object',
    'event_no':"object",
    "prv_term":"object",
    'event_no_float':'float64'
    }
    xlsx_path = os.path.join(module_dir,'tables','all_meta.json')
    with open(xlsx_path) as json_file:
        meta_types = json.load(json_file)
        json_file.close()

    all_meta.update(meta_types)
    
    if pandas_main_version != '1' :
        for key ,value in all_meta.items() :
            if value == 'datetime64[ns]' :
                all_meta.update({key : 'datetime64[ms]'})