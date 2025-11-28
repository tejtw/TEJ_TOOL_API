import os
from TejToolAPI.TejToolAPI import get_history_data
import pandas as pd
# 添加專案根目錄到 Python 路徑，確保使用本地版本
import sys
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)


def test_fin_self_acc_integration():
    """
    確定自結數 = 'Y' 功能正常運作
    """
    data = get_history_data(
        ticker=['2330'],
        columns=['a3100'],
        start='2025-11-07',
        end='2025-11-12',
        transfer_to_chinese=False,
        show_progress=True , 
        fin_type = ['Q'] ,
        include_self_acc = 'Y'
    )

    correct_data = {'coid': {0: '2330', 1: '2330', 2: '2330', 3: '2330'},
                    'mdate': {0: pd.Timestamp('2025-11-07 00:00:00'),
                    1: pd.Timestamp('2025-11-10 00:00:00'),
                    2: pd.Timestamp('2025-11-11 00:00:00'),
                    3: pd.Timestamp('2025-11-12 00:00:00')},
                    'Total_Operating_Income_Q': {0: 989920000.0,
                    1: 989920000.0,
                    2: 989918318.0,
                    3: 989918318.0}}
    correct_df = pd.DataFrame(correct_data)
    assert correct_df.compare(data).size == 0


def test_fin_self_acc_not_integration():
    """
    確定自結數 = 'N' 功能正常運作
    """
    data = get_history_data(
        ticker=['2330'],
        columns=['a3100'],
        start='2025-11-07',
        end='2025-11-12',
        transfer_to_chinese=False,
        show_progress=True , 
        fin_type = ['Q'] ,
    )

    correct_data = {'coid': {0: '2330', 1: '2330', 2: '2330', 3: '2330'},
                    'mdate': {0: pd.Timestamp('2025-11-07 00:00:00'),
                    1: pd.Timestamp('2025-11-10 00:00:00'),
                    2: pd.Timestamp('2025-11-11 00:00:00'),
                    3: pd.Timestamp('2025-11-12 00:00:00')},
                    'Total_Operating_Income_Q': {0: 933791869.0,
                    1: 933791869.0,
                    2: 933791869.0,
                    3: 933791869.0}}
    correct_df = pd.DataFrame(correct_data)
    assert correct_df.compare(data).size == 0


def test_fin_self_acc_code_but_not_Y():
    """
    測試採用自結數代碼但 self_acc != 'Y' 時，依舊可以正常取得資料
    """
    data = get_history_data(
        ticker=['2330'],
        columns=['ip12'],
        start='2025-11-07',
        end='2025-11-12',
        transfer_to_chinese=False,
        show_progress=True , 
        fin_type = ['Q'] ,
        # include_self_acc = 'Y'
    )
    correct_data = {'coid': {0: '2330', 1: '2330', 2: '2330', 3: '2330'},
                    'mdate': {0: pd.    Timestamp('2025-11-07 00:00:00'),
                    1: pd.Timestamp('2025-11-10 00:00:00'),
                    2: pd.Timestamp('2025-11-11 00:00:00'),
                    3: pd.Timestamp('2025-11-12 00:00:00')},
                    'Total_Operating_Income_Q': {0: 933791869.0,
                    1: 933791869.0,
                    2: 933791869.0,
                    3: 933791869.0}}
    correct_df = pd.DataFrame(correct_data)
    assert correct_df.compare(data).size == 0

def test_fin_self_acc_code_also_Y():
    """
    測試採用自結數代碼但 self_acc = 'Y' 時，依舊可以正常取得資料
    """
    data = get_history_data(
        ticker=['2330'],
        columns=['ip12'],
        start='2025-08-11',
        end='2025-08-14',
        transfer_to_chinese=False,
        show_progress=True , 
        fin_type = ['Q'] ,
        include_self_acc = 'Y'
    )
    correct_data = {'coid': {0: '2330', 1: '2330', 2: '2330', 3: '2330'},
                    'mdate': {0: pd.Timestamp('2025-08-11 00:00:00'),
                    1: pd.Timestamp('2025-08-12 00:00:00'),
                    2: pd.Timestamp('2025-08-13 00:00:00'),
                    3: pd.Timestamp('2025-08-14 00:00:00')},
                    'Total_Operating_Income_Q': {0: 933790000.0,
                    1: 933791869.0,
                    2: 933791869.0,
                    3: 933791869.0}}
    correct_df = pd.DataFrame(correct_data)
    assert correct_df.compare(data).size == 0
    