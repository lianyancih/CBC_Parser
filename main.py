from src.controller import download_cbc_data, merge_data, re_sort_csv_by_date, write_csv_file_to_one_file
from src.controller import get_month_url_list
from src.controller import handle_cbc_data
from src.service import process_data_unified_label

# 程式的執行入口
if __name__ == '__main__':
    # # 下載本國銀行資產負債表(全行)XLSX檔案
    # # 設定第一個下載網址: 98年3月底 ~ 112年9月底
    # url = 'https://www.cbc.gov.tw/tw/lp-725-1-1-60.html'
    # url_list = get_month_url_list(url)
    # for i in url_list:
    #     download_cbc_data(i)
    # # 設定第二個下載網址，97年12月底以前
    # url = 'https://www.cbc.gov.tw/tw/lp-725-1-2-60.html'
    # url_list = get_month_url_list(url)
    # for i in url_list:
    #     download_cbc_data(i)
    #
    # print('下載完成')

    # handle_cbc_data()
    # process_data_unified_label()
    merge_data()


