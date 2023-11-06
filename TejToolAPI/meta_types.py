import json
import os
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

    # fin_meta = Fin_meta_types.meta_types
    # stk_meta = Stk_meta_types.meta_types
    # alt_event_meta = Alt_Event_meta_types.meta_types
    
    # all_meta.update(fin_meta)
    # all_meta.update(stk_meta)
    # all_meta.update(alt_event_meta)
    xlsx_path = os.path.join(module_dir,'tables','all_meta.json')
    with open(xlsx_path) as json_file:
        meta_types = json.load(json_file)
        json_file.close()

    all_meta.update(meta_types)
