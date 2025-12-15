## 使用 Tool API 獲取數據(股價、財務、月營收、集保和股票屬性)
### 安裝套件
```python
pip install tej-tool-api
```

### 匯入套件
```python
import os
os.environ['TEJAPI_KEY'] = "YOURAPIKEY"

import TejToolAPI
```
### get_history_data - 獲取歷史資料<br>

```python
list_of_Stocks = ['2330','2303','2454', '2882', '2881']
# 撈取歷史資料
data = TejToolAPI.get_history_data(
ticker=list_of_Stocks,
columns= ['稅前淨利成長率', '單月營收成長率'], 
transfer_to_chinese=False
)
```
目前資料庫僅支援台灣市場。<br>
Tool API 可獲取 PIT 資料庫的所有欄位，共計超過 600 個指標，具體指標內容參考[TEJAPI_量化投資資料庫](https://view.officeapps.live.com/op/view.aspx?src=https%3A%2F%2Ftquant.tejwin.com%2Fwp-content%2Fuploads%2FTEJ-TOOL-API%25E6%25AC%2584%25E4%25BD%258D%25E5%25B0%258D%25E7%2585%25A7%25E6%25B8%2585%25E5%2596%25AE20251126.xlsx&wdOrigin=BROWSELINK) 。<br>
財務數據是根據發布日（announcement date）來mapping，非發布日的財務數據會使用當下可獲得最新的資料為準進行填值。ex: 2330 在 2010-02-01 時所能獲得最新的財務資料為 2009Q3 的財務資料，則 2010-01-01 會以 2009Q3 的資料進行填補。惟公司2009Q4自結財報早於 2010-02-01 發布時，且 include_self_acc = 'Y'，這時 2010-02-01 的財務數據使用自結財務數據。<br>
<br>

**參數:**


| Parameters | Is Required | Data Type | Descriptions |
|------------|-------------|-----------|--------------|
| ticker     | Required    | list      | 股票代碼，ex: ['2330', '2881', '2882'] |
| columns    | Required    | list      | 欄位代碼，可參考[TEJAPI_量化投資資料庫](https://view.officeapps.live.com/op/view.aspx?src=https%3A%2F%2Ftquant.tejwin.com%2Fwp-content%2Fuploads%2FTEJ-TOOL-API%25E6%25AC%2584%25E4%25BD%258D%25E5%25B0%258D%25E7%2585%25A7%25E6%25B8%2585%25E5%2596%25AE20251126.xlsx&wdOrigin=BROWSELINK) |
| start      | Optional    | date/str  | 起始日，ex: '2008-01-01' (預設值為 '2013-01-01')，目前版本尚未支援timezone的設定 |
| end        | Optional    | date/str  | 結束日，ex: '2008-01-01' (預設值為今日 datetime.now())，目前版本尚未支援timezone的設定|
| transfer_to_chinese | Optional | boolean | 欄位轉換成中文，若 transfer_to_chinese=True，則欄位顯示為中文名稱，transfer_to_chinese=False，則欄位顯示為英文名稱。(預設為 False)|
| fin_type   | Optional    | list      | 會計科目類型 -> 單季:Q、累計:A、移動四季:TTM，ex: 撈取單季和累積，fin_type=['Q','A']。(預設為 ['Q','A','TTM']) |
| include_self_acc | Optional | string | 財務是否包含公司自結損益，include_self_acc='Y'，表示財務資料包含自結損益，否則僅有財簽資料 (預設為 'N') |
| npartitions | Optional    | int       | 代表取得資料時已n家公司為一組單位，ex: 取得100家資料時設定 npartitions = 50 ,則會分成 ceil(100/50) = 2 次 分別抓取後再合併 |


### get_internal_code  <br>
內部欄位編碼與中英文簡稱轉換功能

```python
TejToolAPI.get_internal_code(['稅前淨利成長率', 'Gross_Profit_Loss_from_Operations'])

```
```html
output: ['r404', 'gm']
```
<br>

**參數:**

| Parameters | Is Required | Data Type | Descriptions |
|------------|-------------|-----------|--------------|
| columns    | Required    | list      | 欄位（可以輸入"欄位代號"、"中文名稱"或"英文欄位"），可參考[TEJAPI_量化投資資料庫](https://view.officeapps.live.com/op/view.aspx?src=https%3A%2F%2Ftquant.tejwin.com%2Fwp-content%2Fuploads%2FTEJ-TOOL-API%25E6%25AC%2584%25E4%25BD%258D%25E5%25B0%258D%25E7%2585%25A7%25E6%25B8%2585%25E5%2596%25AE20251126.xlsx&wdOrigin=BROWSELINK) |


### search_columns <br>
若想從內部編碼反向取得 columns 所對應之中英文欄位則可利用 search_columns 這個function <br>

```python
TejToolAPI.search_columns(['r404'])
```


| columns | chn_column_names | eng_column_names | table_names | TABLE_NAMES | API_TABLE | CHN_NAMES |
|---------|-----------------|------------------|-------------|--------------|-----------|-----------|
| r404    | 稅前淨利成長率    | Pre_Tax_Income_Growth_Rate | fin_self_acc | fin_self_acc | TWN/AFESTM1 | 財務-自結數 |
| r404    | 稅前淨利成長率    | Pre_Tax_Income_Growth_Rate | fin_board_select | fin_board_select | TWN/AFESTMD | 財務-董事決議數 |

<br>

**參數:**



| Parameters | Is Required | Data Type | Descriptions |
|------------|-------------|-----------|--------------|
| columns    | Required    | list      | 欄位代碼，可參考[TEJAPI_量化投資資料庫](https://view.officeapps.live.com/op/view.aspx?src=https%3A%2F%2Ftquant.tejwin.com%2Fwp-content%2Fuploads%2FTEJ-TOOL-API%25E6%25AC%2584%25E4%25BD%258D%25E5%25B0%258D%25E7%2585%25A7%25E6%25B8%2585%25E5%2596%25AE20251126.xlsx&wdOrigin=BROWSELINK) |

### search_table <br>
```python
TejToolAPI.search_table(['r404'])
```

| COLUMNS | TABLE_NAMES | 
|---------|-----------------|
| r404    | fin_self_acc    |
| r404    | fin_auditor    |

<br>

**參數:**



| Parameters | Is Required | Data Type | Descriptions |
|------------|-------------|-----------|--------------|
| columns    | Required    | list      | 欄位代碼，可參考[TEJAPI_量化投資資料庫](https://view.officeapps.live.com/op/view.aspx?src=https%3A%2F%2Ftquant.tejwin.com%2Fwp-content%2Fuploads%2FTEJ-TOOL-API%25E6%25AC%2584%25E4%25BD%258D%25E5%25B0%258D%25E7%2585%25A7%25E6%25B8%2585%25E5%2596%25AE20251126.xlsx&wdOrigin=BROWSELINK) |


