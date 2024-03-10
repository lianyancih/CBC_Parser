import os
import requests

# # 取得資料夾內所有檔案路徑
def get_data_file_list(path: str) -> list:
    return [path + i for i in os.listdir(path) if os.path.isfile(path + i)]

