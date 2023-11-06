import os
import tejapi
try:
    tejapi.ApiConfig.api_base = os.environ.get('TEJAPI_BASE')
except:
    pass

tejapi.ApiConfig.api_key = os.environ.get('TEJAPI_KEY')
tejapi.ApiConfig.ignoretz = True
tejapi.ApiConfig.page_limit=10000

from .TejToolAPI import (get_history_data,
                         search_table,
                         search_columns,
                         get_internal_code,
                         get_trading_calendar,
                         transfer_language_columns,
                         triggers
                        )

from .meta_types import Meta_Types
                        

from . import Map_Dask_API

