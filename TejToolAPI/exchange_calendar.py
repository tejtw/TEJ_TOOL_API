# import os
import tejapi
# tejapi.ApiConfig.ignoretz = True
# tejapi.ApiConfig.page_limit=10000
# tejapi.ApiConfig.api_base = os.environ.get('TEJAPI_BASE')
# tejapi.ApiConfig.api_key = os.environ.get('TEJAPI_KEY')

import pandas as pd
import numpy as np


class ExchangeCalendar:
    def __init__(self) -> None:
        self.calendar = self.get_trading_calendar()
        self.calendar_list = self.calendar['zdate'].tolist()
        self.date_int = self.calendar['zdate'].values.astype(np.int64)

    def get_trading_calendar(self):
        """
        Extrieve calendar from tejapi-TWN/TRADEDAY_TWSE, retain all trading dates of the calendar.
        """
        calendar = tejapi.fastget('TWN/TRADEDAY_TWSE', 
                        paginate = True,
                        mkt = 'TWSE',
                        date_rmk = '',
                        opts = {'columns':['zdate']}
                        )
        
        return calendar

    def is_session(self, date):
        """ 
        Check if the date is valid for trading.
        ---------------------------------------
        True: `date` is trading date.
        False: `date` is not trading date.
        """
        if self.calendar.loc[0, 'zdate'].tz is None:
            utc = False

        else:
            utc = True    

        return pd.to_datetime(date, utc=utc) in self.calendar_list

    def next_open(self, date):
        """
        To make join process efficient,
        shift non-trading announce date to next open trading date.
        ----------------------------------------------------------
        output: next trading date
        """

        date = pd.Timestamp(date)
        idx = next_divider_idx(self.date_int, date.value)

        return pd.Timestamp(self.date_int[idx])
    
    def prev_open(self, date):
        """
        To make join process efficient,
        shift non-trading announce date to previous open trading date.
        ----------------------------------------------------------
        output: next trading date
        """

        date = pd.Timestamp(date)
        idx = previous_divider_idx(self.date_int, date.value)

        return pd.Timestamp(self.date_int[idx])
    
    def annd_adjusted(self, date, shift_backward=True):
        if self.is_session(date):
            return date
        
        if shift_backward:
            return self.prev_open(date)
        
        return self.next_open(date)

    
def next_divider_idx(dividers: np.ndarray, minute_val: int) -> int:

    divider_idx = np.searchsorted(dividers, minute_val, side="right")
    target = dividers[divider_idx]

    if minute_val == target:
        # if dt is exactly on the divider, go to the next value
        return divider_idx + 1
    else:
        return divider_idx

def previous_divider_idx(dividers: np.ndarray, minute_val: int) -> int:

    divider_idx = np.searchsorted(dividers, minute_val)

    if divider_idx == 0:
        # print(dividers)
        # print(dividers[divider_idx])
        raise ValueError("Cannot go earlier in calendar!")
        # return divider_idx

    return divider_idx - 1