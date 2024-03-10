import csv
import re

import requests
from bs4 import BeautifulSoup

from src.service import convert_to_ad_year, convert_bank_code, \
    merge_all_banks_data_by_code_by_date, merge_tef_df_by_code_by_date
from src.utils import get_data_file_list

import xlrd
import pandas as pd

# 取得目錄頁中的所有內容頁完整網址
def get_month_url_list(url: str) -> list:
    # 取得網頁網頁原始碼
    response = requests.get(url)
    html_text = response.text
    soup = BeautifulSoup(html_text, 'html.parser')
    links = []
    # 取得網頁原始碼中的月份網址
    for li in soup.select('section.lp div.list ul li'):
        a = li.find('a')
        # 取得下載網址
        link = a['href']
        # 取得標題
        title = a['title']
        # 將標題與下載網址加入list
        links.append({
            'title': title,
            'link': link
        })
    # 回傳list
    return links

# 下載內容頁檔案
def download_cbc_data(url):
    # response 是requests get 取得的結果
    print('處理年/月份:', url.get('title'))
    response = requests.get('https://www.cbc.gov.tw' + url.get('link'))
    if response.status_code == 200:  # 確認請求是成功的
        html_text = response.text  # 將下載網址內容放到html_text
        soup = BeautifulSoup(html_text, features='html.parser')  # 解析html結構
        temp = soup.select('div.file_download a[href]')  # 將解析結果暫時放到temp中
        # 檢查是否有資產負債表(全行)的檔案
        for i in temp:
            # 如果有資產負債表(全行)的檔案
            if '本國銀行資產負債' in i.get('title') and '全行' in i.get('title'):  # 條件是嘗試過後的結果
                # 如果檔案是XLSX或XLS
                if 'XLSX' in i.text or 'XLS' in i.text:
                    print(i.get('title'), i.get('href'), i.text)
                    # 下載檔案
                    response = requests.get(i.get('href'))
                    # 指定檔案位置 # 要記得先建立資料夾
                    with open('data/cbc_data/' + url.get('title') + '_' + i.get('title').replace(" ", ""), 'wb') as f:
                        f.write(response.content)
    else:
        print('https://www.cbc.gov.tw' + url.get('link'), " 抓取失敗")


# 處理cbc_data為個別bank_data
def handle_cbc_data():
    bank_list = []
    # 設定資料夾路徑
    folder_path = 'data/cbc_data/'
    # 取得cbc_data中每一個檔案路徑
    file_list = get_data_file_list(folder_path)

    for i in file_list:
        # 顯示進度的log
        print("處理:", i)
        if 'xlsx' in i:
            # 讀取xlsx檔案
            df = pd.read_excel(i, header=7)
        else:
            # 讀取xls檔案
            workbook = xlrd.open_workbook(i)
            # 取得sheet by index 0 第一個sheet
            sheet = workbook.sheet_by_index(0)
            # 取得sheet名稱
            sheet_name = sheet.name
            # 讀取xls檔案
            # 這邊因為有些檔案比較機車 header 位置不一樣所以要個別處理
            if "91年12月底" in i or "92年12月底" in i or "92年3月底" in i or "92年6月底" in i or "92年9月底" in i:
                df = pd.read_excel(i, sheet_name=sheet_name, header=4)
            elif "93年6月底" in i or "93年9月底" in i:
                df = pd.read_excel(i, sheet_name=sheet_name, header=6)
            else:
                df = pd.read_excel(i, sheet_name=sheet_name, header=7)

        date = i.split('/')
        date = date[2].split('_')
        date = date[0]
        for bank_name in df.columns[3:]:
            # 如果銀行名稱已經存在則不處理
            found = False
            # 這邊是要清理錯亂的亂碼
            # 確認bank_name是否在bank_list裡，沒有的話就要append進來
            bank_name = str(bank_name).strip()
            # Loop整個bank_list
            for i in bank_list:
                # 如果銀行名稱已經存在則檢查日期是否存在
                if i.get('bank') == bank_name:
                    # 初始化銀行日期
                    i[date] = {}
                    # 設定found為True
                    found = True
                    # 跳出迴圈
                    break
            # 如果銀行名稱不存在則加入銀行列表
            if not found:
                bank_list.append({'bank': bank_name, date: {}})
        # 將資料轉換成字典 dict
        data_dict = df.to_dict(orient='records')
        # loop資料dict
        for i in data_dict:
            try:
                # loop銀行列表
                for bank in bank_list:
                    # 如果銀行名稱不在i裡面則不處理
                    if bank.get("bank") not in i:
                        continue
                    #  如果銀行名稱在i裡面
                    if i.get(bank.get("bank")):
                        # 取得值
                        value_str = str(i.get(bank.get("bank"))).replace(",", "").replace("-", "").strip()
                        # 如果值不是空白或-或nan 則處理
                        if value_str != "" and value_str != "-" and value_str != "nan":
                            if i.get("項                目"):
                                try:
                                    if date not in bank:
                                        bank[date] = {}
                                        bank[date][str(i.get("項                目")).strip()] = int(float(value_str))
                                    else:
                                        bank[date][str(i.get("項                目")).strip()] = int(float(value_str))
                                except ValueError:
                                    pass
                            elif i.get("項            目") and "註：" not in i.get("項            目"):
                                try:
                                    if date not in bank:
                                        bank[date] = {}
                                        bank[date][str(i.get("項            目")).strip()] = int(float(value_str))
                                    else:
                                        bank[date][str(i.get("項            目")).strip()] = int(float(value_str))
                                except ValueError:
                                    pass
                            elif i.get("項                    目"):
                                try:
                                    if date not in bank:
                                        bank[date] = {}
                                        bank[date][str(i.get("項                    目")).strip()] = int(float(value_str))
                                    else:
                                        bank[date][str(i.get("項                    目")).strip()] = int(float(value_str))
                                except ValueError as e:
                                    pass
            except KeyError:
                print("key error", date)
                pass
    # 產出一個銀行一個檔案
    # for i in bank_list:
    #     # 寫入csv檔案
    #     write_csv_file(i)
    # 將所有銀行合併為一個all_banks_data
    write_csv_file_to_one_file(bank_list)
    # 對all_banks_data 按年月遞增排序
    re_sort_csv_by_date('data/bank_data/all_banks_data.csv')



# 從下載下來的bank_data存出想要資料欄位
def write_csv_file(data: dict):
    # 這邊是因為上面的bank_list 有存了錯誤的名稱，所以這邊要用re.search去掃描字串然後去比對是不是中文
    # “\u4e00”和“\u9fa5”是unicode 中文的編碼開始與結束，用來判斷字串裡面有沒有中文，錯誤的名稱是非中文的所以不會儲存到
    if re.search('[\u4e00-\u9fa5]', data.get("bank")) is not None:
        with open(f'data/bank_data/{data.get("bank")}.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            # 覺得要取哪些欄位資料，先設定欄位名稱
            writer.writerow(
                ['時間', '資產合計', '約定融資額度', '應收保證款項', '應收信用狀款項', '信託資產', '放款承諾責任',
                 '保證責任',
                 '信用狀責任', '信託負債'])

            for date, values in data.items():
                # 如果日期是bank則不處理
                if date == 'bank':
                    continue
                # 取得資產合計
                assert_sum = values.get("資產合計")
                # 如果資產合計是空白則設定為values.get("資產總計")
                if assert_sum is None:
                    assert_sum = values.get("資產總計")
                # 寫入每個欄位資料
                writer.writerow([str(date).replace("年", "/").replace("月底", ""), assert_sum,
                                 values.get("約定融資額度"),
                                 values.get("應收保證款項"),
                                 values.get("應收信用狀款項"), values.get("信託資產"), values.get("放款承諾責任"),
                                 values.get("保證責任"), values.get("信用狀責任"), values.get("信託負債")])

# 跟上面很像，但他是直接將bank_list處理成一個合併所有銀行檔案（最新）
def write_csv_file_to_one_file(bank_list):
    # 取得bank code (銀行代碼）
    bank_code = convert_bank_code()
    # 開啟一個CSV檔案用於寫入所有銀行的資料
    with open('data/bank_data/all_banks_data.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # 寫入標題列
        writer.writerow(
            ['代號', '名稱', '年月', '資產合計', '約定融資額度', '應收保證款項', '應收信用狀款項', '信託資產',
             '放款承諾責任',
             '保證責任', '信用狀責任', '信託負債'])

        for bank in bank_list:
            bank_name = bank.get("bank").replace(" ", "")
            # 過濾掉非中文銀行名稱
            if re.search('[\u4e00-\u9fa5]', bank_name):
                for date, values in bank.items():
                    if date == 'bank':
                        continue
                    assert_sum = values.get("資產合計", values.get("資產總計"))
                    date = str(date).replace("年", "/").replace("月底", "")
                    date = convert_to_ad_year(date)
                    # 寫入每列資料
                    writer.writerow([bank_code.get(bank_name), bank_name, date, assert_sum,
                                     values.get("約定融資額度"), values.get("應收保證款項"),
                                     values.get("應收信用狀款項"),
                                     values.get("信託資產"), values.get("放款承諾責任"), values.get("保證責任"),
                                     values.get("信用狀責任"), values.get("信託負債")])

# 將all_banks_data排序
def re_sort_csv_by_date(csv_file):
    df = pd.read_csv(csv_file)
    # 將'年月'轉換為日期時間格式進行排序
    df['年月'] = pd.to_datetime(df['年月'], format='%Y/%m')
    df = df.groupby('代號', as_index=False).apply(lambda x: x.sort_values('年月')).reset_index(drop=True)
    # 將'年月'轉換回YYYY-MM格式的字符串
    df['年月'] = df['年月'].dt.strftime('%Y/%m')
    # 轉換'代號'列回整數型態，如果沒有缺失值
    if not df['代號'].isnull().any():
        df['代號'] = df['代號'].astype(int)
    df.to_csv(csv_file, index=False)


# 讀取整理好的央行資料以及TEJ資料，進行合併資料處理
def merge_data():
    tej_df = merge_tef_df_by_code_by_date()
    print(tej_df)
    cbc_df = merge_all_banks_data_by_code_by_date()
    # print(cbc_df)
    merged_df = pd.merge(tej_df, cbc_df, on=['代號', '年月'], how='left')  # how='left'參數來確保所有在tej_df中的行都會被保留
    # 將'年月'欄位轉換為日期格式
    merged_df['年月'] = pd.to_datetime(merged_df['年月'], format='%Y/%m')
    # 對每一家銀行的資料根據年月進行排序
    merged_df = merged_df.groupby('代號', as_index=False).apply(lambda x: x.sort_values('年月')).reset_index(drop=True)
    # 輸出檔案
    merged_df.to_csv('./data/merged_data.csv')

