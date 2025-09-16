"""
TEJ Tool API - Dask 平行處理模組

此模組提供基於 Dask 的大規模財務資料處理功能，支援台灣證券交易所的各種金融資料源。
主要功能包括財務會計資料、交易資料、替代資料的高效並行獲取與處理。

模組架構：
- ToolApiMeta: 基礎抽象類別，定義通用的資料處理介面
- FinSelfAccData: 財務自結數專用處理器（未經查核的內部編製財報）
- TradingData: 股價交易資料專用處理器
- AlternativeData: 另類資料（月營收、ESG等）專用處理器
- FinAuditorData: 簽證財務專用處理器（經會計師查核的正式財報）

技術特色：
- 基於 Dask 的分散式並行計算
- 智能記憶體管理和垃圾回收
- 跨 pandas 版本的相容性處理

Author: TEJ Team
Date: 2025
Version: 2.0
"""

import tejapi                           # TEJ API 連接套件
import pandas as pd                     # 資料處理基礎套件
import datetime                         # 日期時間處理
import numpy as np                      # 數值計算套件
import dask.dataframe as dd            # 並行資料框架
import dask                            # 並行計算核心
import gc                              # 垃圾回收管理
import TejToolAPI.parameters as para   # TEJ 參數設定
from TejToolAPI.meta_types import Meta_Types  # 資料型別定義
import warnings

# 全域變數設定
global pandas_main_version, meta_type

# 取得 pandas 主版本號，用於相容性處理
pandas_main_version = pd.__version__.split('.')[0]

# 設定統一的資料型別標準
meta_type = Meta_Types.all_meta

# 忽略不重要的警告訊息
warnings.filterwarnings('ignore')

class ToolApiMeta:
    """
    TEJ API 資料處理的基礎抽象類別
    
    這是所有 TEJ 資料處理類別的基礎架構，提供了統一的介面和通用功能。
    此類別實現了資料獲取、並行處理、版本控制等核心機制。
    
    核心設計理念：
    - 模組化設計：每種資料類型有專用的子類別
    - 並行優化：利用 Dask 實現大規模資料的高效處理
    - 版本控制：自動處理財務資料的多版本問題
    - 記憶體管理：智能垃圾回收，避免記憶體溢出
    
    主要功能模組：
    1. 資料獲取：透過 TEJ API 批量獲取資料
    2. 並行處理：自動分割工作負載到多個核心
    3. 版本管理：處理財務資料的修正和重編
    4. 日期處理：標準化交易日曆和公告日期
    5. 資料轉換：財務頻率透視和格式化    
  
    使用場景：
    - 大規模財務資料分析
    - 投資組合回測系統
    - 風險管理模型
    - 量化交易策略開發
    
    注意事項：
    - 這是抽象基礎類別，不應直接實例化
    - 請使用具體的子類別如 FinSelfAccData
    - 確保有足夠的記憶體進行並行處理
    - 建議根據系統性能調整 npartitions 參數
    """

    def __init__(self, tickers, start, end, extend_fg, npartitions, columns , freq):
        """
        初始化 TEJ API 資料處理器
        
        參數說明：
            tickers (list): 股票代碼清單
                - 格式：['2330', '2317', '2454']
                - 建議：單次處理不超過 500 支股票以避免 API 限制
                
            start (str): 資料起始日期
                - 格式：'YYYY-MM-DD'
                - 範例：'2020-01-01'
                
            end (str): 資料結束日期  
                - 格式：'YYYY-MM-DD'
                - 範例：'2024-12-31'
                
            extend_fg (str): 實驗性資料擴展模式
                - 'Y': 保留所有歷史版本（適合研究用途）
                - 'N': 僅保留最新版本（適合實務應用）
                - 注意：此功能為實驗性質，可能影響效能或資料一致性
                
            npartitions (int): 並行處理分區數
                - 建議：CPU 核心數的 2-4 倍
                - 範圍：1-50，過大會增加管理成本
                
            columns (list): 需要的資料欄位
                - 格式：['eps', 'roe']
                - 可使用中英文欄位名稱
                
            freq (list): 財務資料頻率
                - 選項：['Q', 'A', 'TTM']
                - Q=季報, A=累季, TTM=近四季累計
        """
        # 股票代碼清單
        self.tickers = tickers
        
        # 時間範圍設定
        self._start = start
        self._end = end
        
        # 處理模式設定
        self._extend_fg = extend_fg      # 實驗性：是否保留歷史版本
        self._npartitions = npartitions  # 並行分區數量
        
        # 資料規格設定
        self.columns = columns           # 需要的欄位清單
        self._freq = freq               # 財務資料頻率

    # === 屬性定義區域 ===
    
    @property
    def API_CODE(self):
        """
        API 代碼識別器
        
        Returns:
            str: 空字串，由子類別覆寫定義具體的 API 代碼
            
        Note:
            這是抽象屬性，子類別必須實作：
            - FinSelfAccData: "A0002" (財務會計資料)
            - TradingData: "A0001" (交易資料)  
            - AlternativeData: "A0003" (替代資料)
            - FinAuditorData: "A0004" (稽核資料)
        """
        return ""
    
    @property
    def basic_columns(self):
        """
        基礎必要欄位
        
        Returns:
            list: 所有資料表都必須包含的基礎欄位
            
        Note:
            - coid: 公司代碼（Company ID）
            - mdate: 資料日期（Market Date）
            這些欄位用於資料合併和索引建立
        """
        return ['coid', 'mdate']
    
    @property
    def require_columns(self):
        """
        完整需求欄位清單
        
        Returns:
            list: 基礎欄位 + 使用者指定欄位的組合
            
        Purpose:
            確保資料完整性，避免遺漏關鍵欄位
        """
        return self.basic_columns + self.columns
    
    @property 
    def available_tables(self):
        """
        可用的 API 資料表清單
        
        Returns:
            list: 根據 API_CODE 篩選出的可用資料表
            
        Process:
            1. 根據 API_CODE 查詢對應的 TABLE_NAMES
            2. 從參數配置中獲取實際的 API_TABLE 名稱
            3. 回傳可供 tejapi.fastget() 使用的表格清單
        """
        # 從 API 配置表中查詢對應的表格名稱
        api_table = para.table_API
        tb_names = api_table[api_table['API_CODE'] == self.API_CODE]['TABLE_NAMES'].tolist()
        
        # 將表格名稱轉換為實際的 API 表格代碼
        api_tb_names = para.fin_invest_tables.loc[
            para.fin_invest_tables['TABLE_NAMES'].isin(tb_names), 'API_TABLE'
        ].tolist()
        
        return api_tb_names
    
    @property
    def available_columns(self):
        """
        可用的資料欄位清單
        
        Returns:
            list: 根據 API_CODE 可以獲取的所有欄位名稱
            
        Purpose:
            用於驗證使用者請求的欄位是否存在，避免無效的 API 呼叫
        """
        # 取得對應的表格名稱
        api_table = para.table_API
        tb_names = api_table[api_table['API_CODE'] == self.API_CODE]['TABLE_NAMES'].tolist()
        
        # 查詢這些表格包含的所有欄位
        return para.table_columns[para.table_columns['TABLE_NAMES'].isin(tb_names)].tolist()
    
    # === 核心執行方法 ===
    
    def compute_multi_fetch(self, table):
        """
        抽象方法：多工並行資料獲取
        
        Args:
            table (str): API 資料表名稱
            
        Returns:
            dd.DataFrame: 空的 Dask DataFrame（基礎類別回傳）
            
        Note:
            這是抽象方法，子類別必須實作具體的獲取邏輯：
            - 分割股票清單到多個分區
            - 建立並行處理任務
            - 執行 Dask 分散式計算
            - 處理資料去重和格式化
        """
        return dd.DataFrame()

    def run(self):
        """
        主要執行入口：批次處理所有可用資料表
        
        Returns:
            dict: 以資料表名稱為鍵，Dask DataFrame 為值的字典
            
        Workflow:
            1. 遍歷所有可用的 API 資料表
            2. 對每個表格執行並行資料獲取
            3. 檢查資料是否有內容（避免空資料表）
            4. 組織成字典格式回傳
            5. 主動釋放記憶體避免累積
            
        Memory Management:
            - 每次處理完一個表格後主動清理暫存變數
            - 只保留有資料的表格，忽略空結果
            
        Performance Tips:
            - 適合處理多個相關資料表的情況
            - 單一資料表建議直接使用 compute_multi_fetch()
        """
        data_sets = {}
        
        # 逐一處理每個可用的資料表
        for table in self.available_tables:
            # 執行並行資料獲取
            data = self.compute_multi_fetch(table)
            
            # 檢查是否有實際資料（避免空 DataFrame 佔用記憶體）
            if (data.size.compute() > 0):                
                data_sets[table] = data
                
            # 主動釋放記憶體
            data = []
            
        return data_sets
    
    # === 靜態工具方法 ===
    
    @staticmethod
    def get_available_columns_by_table(API_Table):
        """
        查詢特定 API 資料表的可用欄位
        
        Args:
            API_Table (str): TEJ API 資料表名稱，如 'TWN/AINVFINBS'
            
        Returns:
            list: 該資料表包含的所有欄位名稱清單
            
        Process:
            1. 從 API 表格對照表中查詢對應的內部表格名稱
            2. 從欄位定義表中獲取該表格的所有欄位
            
        Example:
            >>> columns = ToolApiMeta.get_available_columns_by_table('TWN/AINVFINBS')
            >>> print(columns)
            ['coid', 'mdate', 'annd', 'eps', 'roe', ...]
            
        Raises:
            ValueError: 當找不到對應的表格名稱時
        """
        # 查詢 API 表格對應的內部表格名稱
        table_name = para.fin_invest_tables.loc[
            para.fin_invest_tables['API_TABLE'] == API_Table, "TABLE_NAMES"
        ].item()
        
        # 從欄位定義表中獲取該表格的所有欄位
        return para.table_columns[
            para.table_columns['TABLE_NAMES'] == table_name
        ]['COLUMNS'].tolist()
    
    @staticmethod
    def getMRAnnd_np(data):
        """
        獲取最新版本的財務公告資料 (Most Recent Announcement)
        
        這是處理財務資料版本控制的核心方法。由於財務報表可能會有修正版本，
        此方法確保取得每個報告期間的最新正確版本。
        
        Args:
            data (pd.DataFrame): 包含版本資訊的原始財務資料
                必要欄位：['coid', 'key3', 'annd', 'mdate', 'no']
                - coid: 公司代碼
                - key3: 財務頻率 (Q/A/TTM)
                - annd: 公告日期
                - mdate: 報告期間
                - no: 版本號
                
        Returns:
            pd.DataFrame: 處理後的最新版本資料
            
        Algorithm:
            1. 版本標識：建立 "報告期間-版本號" 的唯一標識
            2. 分組最大：按 (公司, 頻率, 公告日) 分組取最新版本
            3. 累積更新：確保時間序列中不會出現倒退的版本
            4. 版本拆解：將版本標識拆回原始的日期和版本號欄位
            5. 格式標準：統一日期時間格式
            
        Business Logic:
            - 同一報告期間可能有多個版本（原始版、修正版）
            - 後續公告的版本應該比之前的新（版本遞增）
            - 避免因為資料延遲導致的版本混亂
            
        Performance:
            - 使用向量化操作提升處理速度
            - 支援大量資料的批次處理
            
        Example:
            >>> # 原始資料可能包含多個版本
            >>> raw_data = pd.DataFrame({
            ...     'coid': ['2330', '2330', '2330'],
            ...     'mdate': ['2023-03-31', '2023-03-31', '2023-03-31'],
            ...     'annd': ['2023-04-15', '2023-04-20', '2023-04-25'],
            ...     'no': ['1', '2', '3'],
            ...     'key3': ['Q', 'Q', 'Q']
            ... })
            >>> latest_data = ToolApiMeta.getMRAnnd_np(raw_data)
            >>> # 結果只保留版本號最大的記錄
        """
        # 步驟 1: 建立版本唯一標識 (報告期間-版本號)
        data['ver'] = data['mdate'].astype(str) + '-' + data['no']
        
        # 步驟 2: 按 (公司, 財務頻率, 公告日) 分組，取最新版本
        data = data.groupby(
            ['coid', 'key3', 'annd'], 
            as_index=False,          # 保持分組欄位為一般欄位
            group_keys=False         # 避免多層索引
        ).max()
        
        # 步驟 3: 確保版本的時間一致性（累積最大值，避免版本倒退）
        data = data.groupby(
            ['coid', 'key3'], 
            group_keys=False
        ).apply(lambda x: np.fmax.accumulate(x, axis=0))
        
        # 步驟 4: 將版本字串拆解回原始欄位
        data = ToolApiMeta._process_version_data(data)
        data = data.drop(columns='ver')
        
        # 步驟 5: 統一日期時間格式（相容不同 pandas 版本）
        data = ToolApiMeta._standardize_datetime_columns(data, ['mdate', 'annd'])
        
        return data
    
    @staticmethod
    def parallelize_ver_process(data):
        """
        並行處理版本字串拆解
        
        這是內部優化方法，使用向量化操作快速處理大量版本字串。
        
        Args:
            data (pd.DataFrame): 包含 'ver' 欄位的資料
            
        Returns:
            pd.DataFrame: 拆解後包含 'mdate' 和 'no' 欄位的資料
            
        Process:
            1. 向量化版本拆解函數提升效能
            2. 建立唯一值映射表減少重複計算
            3. 批次處理所有版本字串
            4. 根據 pandas 版本設定正確的日期格式
            
        Performance Benefits:
            - 向量化操作比迴圈快 10-100 倍
            - 映射表技術避免重複計算
            - 記憶體使用最佳化
        """
        def parallel_process(func, data):
            """建立向量化函數和映射表"""
            vectorized = np.vectorize(func)
            uni_dates = data['ver'].unique()
            result = vectorized(uni_dates)
            dict_map = {uni_dates[i]: result[i] for i in range(len(result))}
            return dict_map
        
        # 處理報告日期 (mdate) 拆解
        mdate_dict = parallel_process(ToolApiMeta.split_mdate, data)
        data['mdate'] = data['ver'].map(mdate_dict)
        
        # 根據 pandas 版本設定正確的日期格式
        if pandas_main_version != 1:
            data['mdate'] = data['mdate'].astype('datetime64[ms]')
        else:
            data['mdate'] = data['mdate'].astype('datetime64[ns]')
            
        # 處理版本號 (no) 拆解
        no_dict = parallel_process(ToolApiMeta.split_no, data)
        data['no'] = data['ver'].map(no_dict)

        return data
        
    @staticmethod
    def _process_version_data(data):
        """
        版本資料處理的包裝方法
        
        Args:
            data (pd.DataFrame): 包含版本資訊的資料
            
        Returns:
            pd.DataFrame: 處理後的資料
            
        Note:
            這是 getMRAnnd_np 方法的內部調用，提供清晰的介面分離
        """
        return ToolApiMeta.parallelize_ver_process(data)

    @staticmethod
    def _standardize_datetime_columns(data, datetime_columns):
        """
        標準化日期時間欄位格式
        
        由於不同版本的 pandas 對日期時間格式有不同的預設值，
        此方法確保跨版本的相容性。
        
        Args:
            data (pd.DataFrame): 需要處理的資料
            datetime_columns (list): 需要標準化的日期欄位名稱
            
        Returns:
            pd.DataFrame: 格式化後的資料
            
        Compatibility:
            - pandas 1.x: 使用 'datetime64[ns]' (奈秒精度)
            - pandas 2.x+: 使用 'datetime64[ms]' (毫秒精度)
        """
        datetime_format = ToolApiMeta._get_datetime_format()
        
        for col in datetime_columns:
            if col in data.columns:
                data[col] = data[col].astype(datetime_format)
        
        return data

    @staticmethod
    def _get_datetime_format():
        """
        獲取適當的日期時間格式
        
        Returns:
            str: 對應 pandas 版本的最佳日期時間格式
            
        Version Mapping:
            - pandas 1.x: 'datetime64[ns]' (向後相容)
            - pandas 2.x+: 'datetime64[ms]' (效能優化)
        """
        return 'datetime64[ns]' if pandas_main_version == '1' else 'datetime64[ms]'

    @staticmethod
    def fin_pivot(df, remain_keys):
        """
        財務頻率資料透視轉換
        
        將縱向的財務頻率資料轉換為橫向的多欄位格式，便於後續分析。
        這是財務資料處理的核心功能之一。
        
        Args:
            df (pd.DataFrame): 包含 key3 (財務頻率) 欄位的資料
            remain_keys (list): 需要保持不變的識別欄位
                通常包含：['coid', 'mdate', 'annd', 'no']
                
        Returns:
            pd.DataFrame: 透視後的寬格式資料
            
        Transformation Logic:
            原始格式 (長格式):
            | coid | mdate    | eps | key3 |
            |------|----------|-----|------|
            | 2330 | 2023-Q1  | 5.0 | Q    |
            | 2330 | 2023-Q1  | 6.2 | A    |
            
            結果格式 (寬格式):  
            | coid | mdate    | eps_Q | eps_A |
            |------|----------|-------|-------|
            | 2330 | 2023-Q1  | 5.0   | 6.2   |
            
        Business Value:
            - 季報 (Q) 和年報 (A) 資料可以在同一行比較
            - 便於計算同比和環比成長率
            - 簡化後續的資料分析和建模
            
        Performance:
            - 使用逐步合併避免大型資料的記憶體問題
            - 支援任意數量的財務頻率
            
        Example:
            >>> # 原始 EPS 資料包含季報和年報
            >>> raw_data = pd.DataFrame({
            ...     'coid': ['2330', '2330'],
            ...     'mdate': ['2023-03-31', '2023-03-31'],
            ...     'eps': [5.0, 20.5],
            ...     'key3': ['Q', 'A']
            ... })
            >>> remain_keys = ['coid', 'mdate']
            >>> result = ToolApiMeta.fin_pivot(raw_data, remain_keys)
            >>> # 結果: eps_Q=5.0, eps_A=20.5 在同一行
        """
        # 取得所有唯一的財務頻率
        unique_frequencies = df['key3'].dropna().unique()
        
        # 如果沒有頻率資料，直接回傳原始資料
        if len(unique_frequencies) == 0:
            return df
        
        # 處理第一個頻率作為基礎
        result_data = ToolApiMeta._pivot_single_frequency(
            df, remain_keys, unique_frequencies[0]
        )
        
        # 逐一合併其他頻率的資料
        for frequency in unique_frequencies[1:]:
            temp_data = ToolApiMeta._pivot_single_frequency(
                df, remain_keys, frequency
            )
            
            # 使用外連接確保所有資料都被保留
            result_data = result_data.merge(temp_data, on=remain_keys, how='outer')
        
        return result_data

    @staticmethod
    def _pivot_single_frequency(df, remain_keys, frequency):
        """
        處理單一財務頻率的透視操作
        
        Args:
            df (pd.DataFrame): 原始資料
            remain_keys (list): 保持欄位清單
            frequency (str): 特定頻率 (如 'Q', 'A', 'TTM')
            
        Returns:
            pd.DataFrame: 該頻率的透視結果
            
        Note:
            這是 fin_pivot 的輔助方法，負責處理單一頻率的轉換邏輯
        """
        return ToolApiMeta.pivot(df, remain_keys, frequency)

    @staticmethod
    def annd_adjusted(exchange_calendar, date, shift_backward=True):
        """
        調整公告日期至營業日
        
        Args:
            exchange_calendar: 交易所日曆物件
            date: 要調整的日期
            shift_backward: True=調整到前一營業日, False=調整到後一營業日
        
        Returns:
            datetime: 調整後的營業日日期
        
        Example:
            輸入: 2023-12-25 (聖誕節，非營業日)
            輸出: 2023-12-22 (前一營業日) 或 2023-12-26 (後一營業日)
        """
        # 如果已經是營業日，直接回傳
        if exchange_calendar.is_session(date):
            return date
        
        # 根據方向調整到最近營業日
        if shift_backward:
            return exchange_calendar.prev_open(date)
        else:
            return exchange_calendar.next_open(date)
        
    @staticmethod
    def parallize_annd_process(data, annd='annd'):
        """
        調整公告日期至營業日
        
        將非營業日的公告日期調整為最近的營業日
        
        Args:
            data: 包含公告日期的 DataFrame
            annd: 公告日期欄位名稱，預設為 'annd'
        
        Returns:
            DataFrame: 調整後的數據
        """
        # 獲取所有唯一的公告日期
        unique_announce_dates = pd.to_datetime(data[annd].dropna().unique())
        
        # 建立向量化的日期調整函數
        vectorized_date_adjuster = np.vectorize(ToolApiMeta.annd_adjusted)
        
        # 批量調整所有日期（調整到前一個營業日）
        adjusted_dates = vectorized_date_adjuster(
            para.exc,           # 交易日曆
            unique_announce_dates,  # 原始日期
            True                # shift_backward=True，調整到前一個營業日
        )
        
        # 建立原始日期到調整日期的映射表
        date_mapping = dict(zip(unique_announce_dates, adjusted_dates))
        
        # 應用映射到數據中
        data[annd] = data[annd].map(date_mapping)
        
        # 根據 pandas 版本設定正確的日期時間格式
        datetime_format = 'datetime64[ns]' if pandas_main_version == '1' else 'datetime64[ms]'
        data[annd] = data[annd].astype(datetime_format)
        
        return data
    
    @staticmethod
    def get_partition_group(tickers, npartitions):
        """
        計算需要的分區數量
        
        Args:
            tickers 股票代碼列表
            npartitions: 每個分區的股票數量
        
        Returns:
            int: 所需的分區數量
        """
        total_tickers = len(tickers)
        basic_partitions = total_tickers // npartitions
        
        # 如果能整除，需要額外加 1 個分區
        # 如果不能整除，需要額外加 2 個分區
        if total_tickers % npartitions == 0:
            return basic_partitions + 1
        else:
            return basic_partitions + 2
        
    @staticmethod
    def get_event_column(table):
        """
        根據表格名稱獲取對應的事件欄位名稱
        
        Args:
            table (str): API 表格名稱
        
        Returns:
            str: 對應的事件欄位名稱
        """
        # 查詢事件欄位配置表，找到對應的事件欄位名稱
        event_column = para.event_column.loc[
            para.event_column['API_TABLE'] == table, 
            'EVENT_COLUMN'
        ].item()
        
        return event_column
    
    @staticmethod
    def get_repo_column(table):
        return para.event_column.loc[para.event_column['API_TABLE'].isin([table]), 'REPO_COLUMN'].item()
    
    @staticmethod
    def rename_alt_event_key3(table):
        if not para.event_column.loc[para.event_column['API_TABLE'].isin([table]), 'KEY3'].isna().item():
            return para.event_column.loc[para.event_column['API_TABLE'].isin([table]), 'KEY3'].item()
        else:
            return ''

    @staticmethod
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
    
    @staticmethod
    def split_mdate(string:str):
        mdate = string.split('-')[:2]
        mdate = pd.to_datetime('-'.join(mdate))
        return mdate
    
    @staticmethod
    def split_no(string:str):
        no = string.split('-')[-1]
        return no
    
    @staticmethod
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
                        )

        
        data = ToolApiMeta.parallize_annd_process(data)
        data['ver'] = data['mdate'].astype(str) + '-' + data['no']
        data = data.groupby(['coid', 'key3','annd'], as_index=False, group_keys = False).max()
        data = data.groupby(['coid','key3'], group_keys = False).apply(lambda x: np.fmax.accumulate(x, axis=0))
        data = ToolApiMeta.parallelize_ver_process(data)
        data = data.drop(columns = 'ver')

        return data

class FinSelfAccData(ToolApiMeta):
    """
    財務自結數資料處理類別
    
    專門處理台灣上市櫃公司的財務自結數（Self-Accounting Financial Data），
    也就是公司自行編製、未經會計師查核的財務報表資料。這類資料通常在
    正式財報公布前提供，具有時效性優勢但準確性相對較低。
        
    支援的資料類型：
    - 自結損益表（營收、毛利、營業利益、稅後淨利等）
    - 自結財務比率（ROE、ROA、EPS 等）
    
    Args:
        tickers (list): 股票代碼列表，如 ['2330', '2317', '2454']
        start (str): 資料起始日期，格式 'YYYY-MM-DD'
        end (str): 資料結束日期，格式 'YYYY-MM-DD'
        freq (list): 財務資料頻率，如 ['Q', 'A', 'TTM']
        extend_fg (str): 實驗性擴展標誌，'Y'=保留所有版本，'N'=僅保留最新版本
            注意：此為實驗性功能，建議使用預設值 'N'
        npartitions (int): 並行處理分區數量，建議根據股票數量調整
        columns (list): 需要獲取的財務指標欄位列表
    
    Example:
        >>> # 獲取台積電和聯發科的自結季報 EPS 資料
        >>> fin_self_acc = FinSelfAccData(
        ...     tickers=['2330', '2454'],
        ...     start='2024-01-01',
        ...     end='2024-12-31',
        ...     freq=['Q', 'A'],
        ...     extend_fg='N',  # 實驗性功能，建議使用預設值
        ...     npartitions=5,
        ...     columns=['eps', 'roe']
        ... )
        >>> data = fin_self_acc.compute_multi_fetch('TWN/AINVFINBS')
        >>> # 結果包含最新的自結財務資料，可能早於正式財報 1-2 個月
        >>> # 結果包含 eps_Q, eps_A, roe_Q, roe_A 等欄位
    """
    def __init__(self, tickers, start, end, freq, extend_fg, npartitions, columns):
        super().__init__(tickers, start, end, extend_fg, npartitions, columns, freq)
    
    @property
    def API_CODE(self):
        return "A0002"
    @property
    def basic_columns(self):
        return ['coid', 'mdate', 'annd', 'no', 'key3', 'sem', 'curr', 'fin_ind']
    @property
    def excluded_freq_columns(self):
        return ['coid', 'mdate', 'annd', 'no', 'key3', 'sem', 'curr', 'fin_ind']
    def columns(self):
        return list(set(self.columns).intersection(set(self.available_columns)))
    def fetch_data(self , table , tickers) -> pd.DataFrame :
        """獲取財務數據"""
        target_columns = list(
            set(self.get_available_columns_by_table(table)).intersection(set(self.columns))
            )
        if len(target_columns) < 1 :
            return pd.DataFrame(columns = self.basic_columns)
        
        target_columns = self.basic_columns + target_columns
        
        data_sets = tejapi.fastget(
            table,
            coid=tickers,
            key3=self._freq,
            paginate=True,
            chinese_column_name=False,
            mdate={'gte': self._start, 'lte': self._end},
            opts={'columns': target_columns}
        )
        columns = data_sets.columns
        new_columns = []
        # 設計頻率規格
        for column in columns :
            if column in self.excluded_freq_columns:
                new_columns.append(column)
            else:
                for fq in self._freq:
                    new_columns.append(column+'_'+fq)
        
        # 設計欄位資料型態
        dtype_dict = {i:pd.Series(dtype=meta_type[i]) for i in new_columns}
        dtype_df = pd.DataFrame(dtype_dict)
        # 若無資料，則回傳空的 dataframe
        if len(data_sets) < 1:
            return dtype_df
        
        temp = data_sets[['coid', 'key3','annd', 'mdate', 'no']].copy()
        fin_date = self.getMRAnnd_np(temp)
        # 釋放記憶體
        temp = []
        del temp
        # merge 資料回來
        data_sets = fin_date.merge(data_sets, how = 'left', on = ['coid', 'key3','annd', 'mdate', 'no'])

        # 移除key3,變成column pivot
        key = self.excluded_freq_columns.copy()
        key.remove('key3')
        dtype_df.drop(columns = 'key3', inplace = True)
        data_sets = self.fin_pivot(data_sets, remain_keys=key)

        # Ensure there is no difference between data_sets and alt_dfs.
        col_diff = set(data_sets.columns).difference(set(dtype_df.columns))
        
        # If difference occurred, update alt_dfs.
        if len(col_diff)>0:
            dtype_dict.update({i:data_sets[i].dtypes.name for i in list(col_diff)})
            dtype_df = pd.DataFrame(dtype_dict)
        
        # Fixed the order of the columns
        data_sets = data_sets[dtype_df.columns]
        data_sets = self.parallize_annd_process(data_sets)

        return data_sets

    def compute_multi_fetch(self , table):
        """
        並行獲取多個股票組的財務數據
        
        工作流程：
        1. 計算所需分區數量
        2. 建立並行任務
        3. 執行並行處理
        4. 去除重複資料（可選）
        5. 調整分區數量
        
        Returns:
            dd.DataFrame: 處理後的財務數據
        """
        # 步驟 1: 計算總分區數
        # 例如：100隻股票，每分區10隻 → 需要11個分區
        ticker_partitions = self.get_partition_group(
            tickers=self.tickers,
            npartitions=self._npartitions
        )

        # 步驟 2: 建立並行處理任務
        # 將股票列表分割並為每個子集建立獨立的處理任務
        multi_subsets = [
            dask.delayed(self.fetch_data)(
                table = table , 
                tickers=self.tickers[
                    (i-1)*self._npartitions:    # 分區起始位置
                    i*self._npartitions         # 分區結束位置
                ]
            )
            for i in range(1, ticker_partitions)  # 從分區1開始處理
        ]

        # 步驟 3: 合併所有並行任務的結果
        # 將分散的任務結果組合成單一的 Dask DataFrame
        data_sets = dd.from_delayed(multi_subsets)

        # 步驟 4: 條件性去重處理
        if self._extend_fg == "N":          
            data_sets = data_sets.drop_duplicates(
                subset=['coid', 'annd'],    # 依據：公司代碼 + 公告日期
                keep='last'                 # 策略：保留最新記錄
            )
        
        # 步驟 5: 優化分區配置
        # 確保有足夠的分區數以達到最佳並行處理效能
        if data_sets.npartitions < self._npartitions:
            data_sets = data_sets.repartition(npartitions=self._npartitions)

        return data_sets
    
class TradingData(ToolApiMeta):
    """
    交易資料處理類別
    
    專門處理台灣股市的交易相關資料，包括股價、成交量、技術指標等市場交易資訊。
    此類別繼承自 ToolApiMeta，專注於提供高頻交易資料的獲取和處理功能。
    
    主要功能：
    - 獲取股票日交易資料（開高低收價、成交量等）
    
    支援的資料類型：
    - 日線交易資料（開盤價、最高價、最低價、收盤價、成交量）
    
    特色功能：
    - 高效能的交易資料批量處理
    - 自動資料品質檢查和清理
    - 支援多種時間區間的資料獲取
    - 交易日曆整合，自動處理非交易日
    
    Args:
        tickers (list): 股票代碼列表，如 ['2330', '2317', '2454']
        start (str): 資料起始日期，格式 'YYYY-MM-DD'
        end (str): 資料結束日期，格式 'YYYY-MM-DD'
        freq (list): 資料頻率（對交易資料通常不適用）
        extend_fg (str): 實驗性擴展標誌，'Y'=保留所有資料，'N'=去重處理
            注意：此為實驗性功能，可能影響資料處理效能
        npartitions (int): 並行處理分區數量
        columns (list): 需要獲取的交易資料欄位列表
    
    Example:
        >>> # 獲取台積電和聯發科的日線交易資料
        >>> trading_data = TradingData(
        ...     tickers=['2330', '2454'],
        ...     tables='TWN/APIPRCD',
        ...     start='2024-01-01',
        ...     end='2024-12-31',
        ...     freq=None, # 非財務資訊不適用
        ...     extend_fg='N',  # 實驗性功能，建議保持預設
        ...     npartitions=10,
        ...     columns=['open_d', 'high_d', 'low_d', 'close_d', 'volume']
        ... )
        >>> data = trading_data.compute_multi_fetch('TWN/APIPRCD')
        >>> # 結果包含完整的日線交易資料
    """
    def __init__(self, tickers, start, end, freq, extend_fg, npartitions, columns):
        super().__init__(tickers, start, end, extend_fg, npartitions, columns, freq)

    @property
    def API_CODE(self):
        return "A0001"
    
    def columns(self):
        return list(set(self.columns).intersection(set(self.available_columns)))

    def fetch_data(self , table , tickers) -> pd.DataFrame :
        target_columns = list(
            set(self.get_available_columns_by_table(table)).intersection(set(self.columns))
            )
        if (len(target_columns) < 1) :
            return pd.DataFrame(columns = self.basic_columns)
        target_columns = target_columns + self.basic_columns
        data_sets = tejapi.fastget(
            table,
            coid=tickers,
            paginate=True,
            mdate={'gte': self._start, 'lte': self._end},
            opts={'columns':  target_columns}
        )
        
        # 設計欄位資料型態
        dtype_dict = {i:pd.Series(dtype=meta_type[i]) for i in target_columns}
        dtype_df = pd.DataFrame(dtype_dict)
        # 若無資料，則回傳空的 dataframe
        if len(data_sets) < 1:
            return dtype_df
        # Ensure there is no difference between data_sets and alt_dfs.
        col_diff = set(data_sets.columns).difference(set(dtype_df.columns))

        # If difference occurred, update alt_dfs.
        if len(col_diff)>0:
            dtype_dict.update({i:data_sets[i].dtypes.name for i in list(col_diff)})
            dtype_df = pd.DataFrame(dtype_dict)

        # Fixed the order of the columns
        data_sets = data_sets[dtype_df.columns]
        
        return data_sets
    
    def compute_multi_fetch(self , table):
        """
        並行獲取多個股票組的財務數據
        
        工作流程：
        1. 計算所需分區數量
        2. 建立並行任務
        3. 執行並行處理
        4. 去除重複資料（可選）
        5. 調整分區數量
        
        Returns:
            dd.DataFrame: 處理後的財務數據
        """
        # 步驟 1: 計算總分區數
        # 例如：100隻股票，每分區10隻 → 需要11個分區
        ticker_partitions = self.get_partition_group(
            tickers=self.tickers,
            npartitions=self._npartitions
        )

        # 步驟 2: 建立並行處理任務
        # 將股票列表分割並為每個子集建立獨立的處理任務
        multi_subsets = [
            dask.delayed(self.fetch_data)(
                table = table , 
                tickers=self.tickers[
                    (i-1)*self._npartitions:    # 分區起始位置
                    i*self._npartitions         # 分區結束位置
                ]
            )
            for i in range(1, ticker_partitions)  # 從分區1開始處理
        ]
        
        # 步驟 3: 合併所有並行任務的結果
        # 將分散的任務結果組合成單一的 Dask DataFrame
        data_sets = dd.from_delayed(multi_subsets)

        # 步驟 4: 條件性去重處理
        if self._extend_fg == "N":          
            data_sets = data_sets.drop_duplicates(
                subset=['coid', 'mdate'],    # 依據：公司代碼 + 公告日期
                keep='last'                 # 策略：保留最新記錄
            )
        # 步驟 5: 優化分區配置
        # 確保有足夠的分區數以達到最佳並行處理效能
        if data_sets.npartitions < self._npartitions:
            data_sets = data_sets.repartition(npartitions=self._npartitions)
        
        return data_sets
    
class AlternativeData(ToolApiMeta):
    """
    替代資料處理類別
    
    專門處理非傳統財務和交易資料的替代資料來源，包括月營收、分析師預測、
    ESG 資料、公司治理指標等替代投資資訊。此類別繼承自 ToolApiMeta，
    提供多元化的另類資料獲取和處理功能。
    
    主要功能：
    - 獲取月營收資料
    - 支援特殊事件
    
    支援的資料類型：
    - 月營收資料（營收金額、年增率、月增率等）
    - 公司治理資料（董事會結構、股權結構等）
    - 特殊事件資料（併購、分割、重大投資等）
    
    特色功能：
    - 自動處理不規則公告日期
    - 智能事件日期對齊到交易日曆
    - 支援多種頻率的資料整合
    - 自動處理資料版本控制和更新
    - 特殊欄位重命名和格式化
    
    Args:
        tickers (list): 股票代碼列表，如 ['2330', '2317', '2454']
        start (str): 資料起始日期，格式 'YYYY-MM-DD'
        end (str): 資料結束日期，格式 'YYYY-MM-DD'
        freq (list): 資料頻率（部分替代資料適用）
        extend_fg (str): 實驗性擴展標誌，'Y'=保留所有版本，'N'=僅保留最新
            注意：此為實驗性功能，使用前請詳閱技術文件
        npartitions (int): 並行處理分區數量
        columns (list): 需要獲取的替代資料欄位列表
    
    Example:
        >>> # 獲取台積電和聯發科的月營收資料
        >>> alt_data = AlternativeData(
        ...     tickers=['2330', '2454'],
        ...     start='2024-01-01',
        ...     end='2024-12-31',
        ...     freq=None, # 非財務資訊不適用
        ...     extend_fg='N',  # 實驗性功能，請謹慎使用
        ...     npartitions=5,
        ...     columns=['Sales_Monthly', 'YoY_Monthly_Sales', 'Sales_Accu_12M']
        ... )
        >>> data = alt_data.compute_multi_fetch('TWN/APISALE1')
        >>> # 結果包含月營收及其成長率資料
    """
    def __init__(self, tickers, start, end, freq, extend_fg, npartitions, columns):
        super().__init__(tickers, start, end, extend_fg, npartitions, columns, freq)

    @property
    def API_CODE(self):
        return "A0003"
    
    def columns(self):
        return list(set(self.columns).intersection(set(self.available_columns)))

    def fetch_data(self , table , tickers) -> pd.DataFrame :
        target_columns = list(
            set(self.get_available_columns_by_table(table)).intersection(set(self.columns))
            )
        if (len(target_columns) < 1) :
            return pd.DataFrame(columns = self.basic_columns) 
        
        target_columns = self.basic_columns + target_columns

        ann_date = self.get_event_column(table)
        if (ann_date not in target_columns) :
            target_columns += [ann_date]
        days = para.exc.calendar
        days = days.rename(columns = {'zdate':'all_dates'})
        # monthly revenue
        if table == 'TWN/APISALE1' :
            if 'key3' not in target_columns :
                new_column = target_columns + ['key3']
            else :
                new_column = target_columns
            data_sets = tejapi.fastget(table,
                        coid = tickers,
                        paginate = True,
                        chinese_column_name=False,
                        mdate = {'gte':self._start,'lte':self._end},
                        opts = {'columns':new_column}
                        )
            data_sets = data_sets.sort_values(by = ['coid' , 'annd_s' , 'mdate' , 'key3'] ,ascending = True)
            dfgb = data_sets.groupby(['coid'])
            while any([i for i in dfgb['mdate'].apply(lambda x: x.diff()).fillna(pd.Timedelta(days = 0)).values if i < 0]) :
                data_sets['diff'] = dfgb['mdate'].apply(lambda x: x.diff()).values
                data_sets.fillna({'diff' : pd.Timedelta(days= 0 )} , inplace= True)
                data_sets = data_sets.loc[data_sets['diff'] >= pd.Timedelta(0)]
                dfgb = data_sets.groupby(['coid'])
            if 'diff' in data_sets.columns :
                del data_sets['diff']
        else :
            # alternative data
            data_sets = tejapi.fastget(table,
                            coid = tickers,
                            paginate = True,
                            chinese_column_name=False,
                            mdate = {'gte':self._start,'lte':self._end},
                            opts = {'columns':target_columns})
            
        new_columns = []
        for i in target_columns:
            if i == ann_date:
                new_columns.append(ToolApiMeta.get_repo_column(table))
            elif i == 'key3' and ToolApiMeta.rename_alt_event_key3(table)!='':
                new_columns.append(ToolApiMeta.rename_alt_event_key3(table))
            else:
                new_columns.append(i)
        # Create the empty dataframe with columns and the corresponding types.
        alt_df = ({'all_dates': pd.Series(dtype=meta_type['all_dates'])})
        alt_df.update({i:pd.Series(dtype=meta_type[i]) for i in new_columns})
        alt_dfs = pd.DataFrame(alt_df)

        if len(data_sets) < 1:    
            return alt_dfs
        
        data_sets = self.parallize_annd_process(data_sets, annd= ann_date)

        data_sets = dd.merge(days, data_sets, how='inner', left_on = ['all_dates'], right_on=[ann_date])
        
        if ann_date!='mdate':
            data_sets = data_sets.rename(columns = {ann_date:self.get_repo_column(table)})

        if self.rename_alt_event_key3(table)!='':
            data_sets = data_sets.rename(columns={'key3':self.rename_alt_event_key3(table)})

        # Ensure there is no difference between data_sets and alt_dfs.
        col_diff = set(data_sets.columns).difference(set(alt_dfs.columns))

        # If difference occurred, update alt_dfs.
        if len(col_diff)>0:
            alt_df.update({i:data_sets[i].dtypes.name for i in list(col_diff)})
            alt_dfs = pd.DataFrame(alt_df)
        
        # Fix the order of columns
        data_sets = data_sets[alt_dfs.columns]
        
        if not isinstance(data_sets , pd.DataFrame) :
            data_sets = data_sets.compute(meta = meta_type)
        
        return data_sets
    
    def compute_multi_fetch(self , table):
        """
        並行獲取多個股票組的財務數據
        
        工作流程：
        1. 計算所需分區數量
        2. 建立並行任務
        3. 執行並行處理
        4. 去除重複資料（可選）
        5. 調整分區數量
        
        Returns:
            dd.DataFrame: 處理後的財務數據
        """
        # 步驟 1: 計算總分區數
        # 例如：100隻股票，每分區10隻 → 需要11個分區
        ticker_partitions = self.get_partition_group(
            tickers=self.tickers,
            npartitions=self._npartitions
        )

        # 步驟 2: 建立並行處理任務
        # 將股票列表分割並為每個子集建立獨立的處理任務
        multi_subsets = [
            dask.delayed(self.fetch_data)(
                table = table , 
                tickers=self.tickers[
                    (i-1)*self._npartitions:    # 分區起始位置
                    i*self._npartitions         # 分區結束位置
                ]
            )
            for i in range(1, ticker_partitions)  # 從分區1開始處理
        ]

        # 步驟 3: 合併所有並行任務的結果
        # 將分散的任務結果組合成單一的 Dask DataFrame
        data_sets = dd.from_delayed(multi_subsets)
        # 步驟 4: 條件性去重處理
        if self._extend_fg == "N" and self.get_repo_column(table) in data_sets.columns :
            data_sets = data_sets.drop_duplicates(
                subset=['coid' , self.get_repo_column(table)],    # 依據：公司代碼 + 公告日期
                keep='last'                 # 策略：保留最新記錄
            )
        
        # 步驟 5: 優化分區配置
        # 確保有足夠的分區數以達到最佳並行處理效能
        if data_sets.npartitions < self._npartitions:
            data_sets = data_sets.repartition(npartitions=self._npartitions)
        
        return data_sets

class FinAuditorData(ToolApiMeta):
    """
    簽證財務資料處理類別
    
    專門處理經會計師簽證查核的正式財務報表資料，這是經過獨立第三方
    會計師事務所查核認證的官方財務資訊。相比於財務自結數，簽證財務
    資料具有更高的準確性和權威性。
    
    簽證財務特色：
    - 權威性：經獨立會計師查核，符合會計準則要求
    - 準確性：經過嚴格的查核程序，數據可靠度高
    - 完整性：包含完整的財務報表和附註說明
    - 法定性：符合法規要求，可作為正式財務依據
    - 滯後性：公布時間較自結數晚 1-3 個月
    
    主要功能：
    - 獲取經會計師簽證的正式財務報表
    - 處理不同頻率的簽證財務資料（季報、年報、半年報）
    - 自動處理財務資料的版本控制和修正
    - 提供完整的會計師查核資訊
    - 支援大量股票的並行資料處理
    
    支援的資料類型：
    - 簽證損益表（經查核的營收、成本、費用、淨利等）
    - 簽證資產負債表（總資產、負債、股東權益等）
    - 簽證現金流量表（營運、投資、融資現金流等）
    - 簽證財務比率（ROE、ROA、負債比率等）
    
    資料品質特點：
    - 準確性：最高，經獨立查核驗證
    - 完整性：最完整，包含所有必要財務資訊
    - 一致性：符合會計準則，具可比較性
    - 及時性：相對較慢，但提供最終確定數據
    
    查核程序包含：
    - 實質性測試程序
    - 內控制度評估
    - 分析性復核程序
    - 管理階層聲明書
    - 獨立性確認
    
    使用場景：
    - 正式的財務分析和評估
    - 投資決策的主要依據
    - 信用評等和風險評估
    - 法規遵循和報告需求
    - 與自結數的差異分析
    
    Args:
        tickers (list): 股票代碼列表，如 ['2330', '2317', '2454']
        start (str): 資料起始日期，格式 'YYYY-MM-DD'
        end (str): 資料結束日期，格式 'YYYY-MM-DD'
        freq (list): 簽證資料頻率，通常為 ['Q', 'A', 'H']
        extend_fg (str): 實驗性擴展標誌，'Y'=保留歷史版本，'N'=僅最新版本
            注意：此為實驗性功能，可能在未來版本中異動或移除
        npartitions (int): 並行處理分區數量
        columns (list): 需要獲取的簽證財務欄位列表
    
    Example:
        >>> # 獲取台積電和聯發科的簽證年報資料
        >>> fin_auditor = FinAuditorData(
        ...     tickers=['2330', '2454'],
        ...     start='2020-01-01',
        ...     end='2024-12-31',
        ...     freq=['A', 'Q'],
        ...     extend_fg='N',  # 實驗性功能，可能影響效能
        ...     npartitions=5,
        ...     columns=['eps', 'roe']
        ... )
        >>> data = fin_auditor.compute_multi_fetch('TWN/AFINAUDIT')
        >>> # 結果包含經會計師簽證的財務資料
    
    """
    def __init__(self, tickers, start, end, freq, extend_fg, npartitions, columns):
        super().__init__(tickers, start, end, extend_fg, npartitions, columns, freq)

    @property
    def API_CODE(self):
        return "A0004"
    @property
    def excluded_freq_columns(self):
        return ['coid', 'mdate', 'key3', 'annd', 'no', 'sem', 'merg', 'curr', 'fin_ind']
    @property
    def basic_columns(self):
        return ['coid', 'mdate', 'key3', 'annd', 'no', 'sem', 'merg', 'curr', 'fin_ind']
    def columns(self):
        return list(set(self.columns).intersection(set(self.available_columns)))
    def fetch_data(self , table , tickers) -> pd.DataFrame :
        target_columns = list(
            set(self.get_available_columns_by_table(table)).intersection(set(self.columns))
            )
        if (len(target_columns) < 1) :
            return pd.DataFrame(columns = self.basic_columns)
        target_columns = self.basic_columns + target_columns
        data_sets = tejapi.fastget(
            table,
            coid = tickers,
            key3 = self._freq, 
            paginate = True,
            chinese_column_name=False,
            mdate = {'gte':self._start,'lte':self._end},
            opts= {'columns': target_columns}
        )
        new_columns = []
        # 設計頻率規格
        for column in target_columns :
            if column in self.excluded_freq_columns:
                new_columns.append(column)
            else:
                for fq in self._freq:
                    new_columns.append(column+'_'+fq)

        lower_new_columns = {i:i.lower() for i in new_columns}
        fix_col_dict = {i:pd.Series(dtype=meta_type[i]) for i in lower_new_columns}
        fix_col_df = pd.DataFrame(fix_col_dict)
        
        if len(data_sets) < 1:
            return fix_col_df
        
        # modify the name of the columns from upper case to lower case.
        lower_columns = {i:i.lower() for i in data_sets.columns}
        data_sets = data_sets.rename(columns=lower_columns)
        
        # get most recent announce date of the company
        temp = data_sets[['coid', 'key3','annd', 'mdate', 'no']].copy()
        fin_date = self.getMRAnnd_np(temp)
        data_sets = fin_date.merge(data_sets, how = 'left', on = ['coid', 'key3','annd', 'mdate', 'no'])

        
        # Select columns
        try:
            data_sets = data_sets.loc[:,target_columns]
        except:
            # Check selected columns exist in the columns of dataframe
            selected_columns = [i for i in target_columns if i in data_sets.columns]
            
            # Ensure `data_sets` does not have duplicate rows.
            selected_columns = list(set(selected_columns))
            
            # Select columns again
            data_sets = data_sets.loc[:,selected_columns]

        # Parallel fin_type (key3) to columns
        fin_keys = self.excluded_freq_columns.copy()
        fin_keys.remove('key3')
        # keys = [i for i in target_columns if i in fin_keys]
        data_sets = self.fin_pivot(data_sets, remain_keys=fin_keys)
        fix_col_df.drop(columns = 'key3' , inplace = True)


        # Ensure there is no difference between data_sets and alt_dfs.
        col_diff = set(data_sets.columns).difference(set(fix_col_df.columns))

        # If difference occurred, update alt_dfs.
        if len(col_diff)>0:
            fix_col_dict.update({i:data_sets[i].dtypes.name for i in list(col_diff)})
            fix_col_df = pd.DataFrame(fix_col_dict)

        # Fixed the order of the columns
        data_sets = data_sets[fix_col_df.columns]
        
        data_sets = self.parallize_annd_process(data_sets)
        
        return data_sets    
    
    def compute_multi_fetch(self , table):
        """
        並行獲取多個股票組的財務數據
        
        工作流程：
        1. 計算所需分區數量
        2. 建立並行任務
        3. 執行並行處理
        4. 去除重複資料（可選）
        5. 調整分區數量
        
        Returns:
            dd.DataFrame: 處理後的財務數據
        """
        # 步驟 1: 計算總分區數
        # 例如：100隻股票，每分區10隻 → 需要11個分區
        ticker_partitions = self.get_partition_group(
            tickers=self.tickers,
            npartitions=self._npartitions
        )

        # 步驟 2: 建立並行處理任務
        # 將股票列表分割並為每個子集建立獨立的處理任務
        multi_subsets = [
            dask.delayed(self.fetch_data)(
                table = table , 
                tickers=self.tickers[
                    (i-1)*self._npartitions:    # 分區起始位置
                    i*self._npartitions         # 分區結束位置
                ]
            )
            for i in range(1, ticker_partitions)  # 從分區1開始處理
        ]

        # 步驟 3: 合併所有並行任務的結果
        # 將分散的任務結果組合成單一的 Dask DataFrame
        data_sets = dd.from_delayed(multi_subsets)

        # 步驟 4: 條件性去重處理
        if self._extend_fg == "N":          
            data_sets = data_sets.drop_duplicates(
                subset=['coid','annd'],    # 依據：公司代碼 + 公告日期
                keep='last'                 # 策略：保留最新記錄
            )
        
        # 步驟 5: 優化分區配置
        # 確保有足夠的分區數以達到最佳並行處理效能
        if data_sets.npartitions < self._npartitions:
            data_sets = data_sets.repartition(npartitions=self._npartitions)

        return data_sets

