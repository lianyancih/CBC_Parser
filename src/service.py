# Import required packages
import pandas as pd
import os



# 轉換 CSV 檔案中的時間格式
def convert_to_ad_year(date_str):
    year, month = date_str.split('/')
    ad_year = int(year) + 1911
    return f'{ad_year}/{month}'

# 讀取銀行代碼轉成key-value的dictionary
def convert_bank_code():
    csv_path = 'data/銀行代碼.csv'
    csv_df = pd.read_csv(csv_path)
    bank_dict = pd.Series(csv_df.Code.values, index=csv_df.銀行).to_dict()
    return bank_dict

if __name__ == '__main__':
    # # 讀取 CSV 檔案
    # csv_path = '../data/bank_data/all_banks_data.csv'
    # csv_df = pd.read_csv(csv_path)
    #
    # # 讀取 TEJ-CSV檔案
    # csv_tej_path = '../隨便隨便.csv'
    # csv_tej_df = pd.read_csv(csv_tej_path)
    #
    # # 顯示檔案前幾行，尤其是關注時間格式
    # csv_head = csv_df.head()
    # csv_tej__head = csv_tej_df.head()
    #
    # # print(csv_head)
    # # print(csv_tej__head)
    #
    # csv_df['時間'] = csv_df['時間'].apply(convert_to_ad_year)
    #
    # # 檢查轉換後的時間格式
    # # print(csv_df)
    #
    # # Rename CSV 欄位名稱
    # csv_df.rename(columns={'時間': '年月', "銀行名稱": "名稱"}, inplace=True)
    # # print(csv_df)
    print(convert_bank_code())

# 統一欄位名稱
def process_data_unified_label():
    df = pd.read_csv('./data/bank_data/all_banks_data.csv')
    # Renaming columns based on their order and the provided description for clarity
    columns_mapping = {
        "Unnamed: 22": "約定融資額度",
        "Unnamed: 23": "應收保證款項",
        "Unnamed: 24": "應收信用狀款項",
        "Unnamed: 25": "信託資產",
        "Unnamed: 26": "放款承諾責任",
        "Unnamed: 27": "保證責任",
        "Unnamed: 28": "信用狀責任",
        "Unnamed: 29": "信託負債"
    }
    df.rename(columns=columns_mapping, inplace=True)

    # Fill NaN values with 0 for calculation
    df.fillna({col: 0 for col in columns_mapping.values()}, inplace=True)

    # Map the values from the latter four columns to the former four and remove the latter four columns
    for src_col, target_col in zip(["放款承諾責任", "保證責任", "信用狀責任", "信託負債"],
                                   ["約定融資額度", "應收保證款項", "應收信用狀款項", "信託資產"]):
        df[target_col] += df[src_col]

    # Remove the latter four columns
    df = df.drop(columns=["放款承諾責任", "保證責任", "信用狀責任", "信託負債"])

    # Save the new DataFrame to a new csv file
    new_file_path = './data/bank_data/all_banks_data_unified.csv'
    df.to_csv(new_file_path, index=False)

# 處理all_bank_data（cbc)，以年月合併合併
def merge_all_banks_data_by_code_by_date():
    cbc_df = pd.read_excel('./data/bank_data/all_banks_data_unified.xlsx', header=1)
    # 從cbc_df中移除'名稱'欄位
    cbc_df = cbc_df.drop(columns=['名稱'])
    # 在cbc_df中，根據'代號'及'年月'分組，並對除了'代號'及'年月'以外的欄位進行加總
    cbc_df_grouped = cbc_df.groupby(['代號', '年月'], as_index=False).sum()
    return cbc_df_grouped

# 處理TEJ資料
def merge_tef_df_by_code_by_date():
    tej_df = pd.read_excel('./data/隨便隨便.xlsx', header=1)
    merged_df = merge_bank_data(tej_df, [2860, 5841], 5841, '中信銀')
    merged_df = merge_bank_data(merged_df, [2824, 5843], 5843, '兆豐銀行')
    merged_df = merge_bank_data(merged_df, [2831, 5872], 5872, '匯豐台灣')
    merged_df = merge_bank_data(merged_df, [2822, 5854], 5854, '合庫')
    merged_df = merge_bank_data(merged_df, [2835, 5835], 5835, '國泰世華')
    merged_df = merge_bank_data(merged_df, [2896, 5835], 5835, '國泰世華')
    merged_df = merge_bank_data(merged_df, [2847, 5852], 5852, '元大銀')
    merged_df = merge_bank_data(merged_df, [2808, 5849], 5852, '永豐銀行')
    merged_df = merge_bank_data(merged_df, [2846, 2838], 2838, '聯邦銀')
    merged_df = merge_bank_data(merged_df, [5861, 5858], 5858, '臺銀')
    merged_df = merge_bank_data(merged_df, [5818, 5870], 5870, '花旗台灣')
    merged_df = merge_bank_data(merged_df, [2898, 2895], 2895, '陽信銀')
    merged_df = merge_bank_data(merged_df, [2810, 5847], 5847, '玉山銀')
    return merged_df


def merge_bank_data(df, merge_codes, target_code, target_name):
    """
    Merge data for specified bank codes into a single target bank code and name without affecting other data.

    Parameters:
    - df: DataFrame containing the bank data.
    - merge_codes: List of bank codes to be merged.
    - target_code: The target bank code after merging.
    - target_name: The target bank name after merging.

    Returns:
    - DataFrame with merged bank data and unaffected other data.
    """
    # Step 1: Filter data that should not be merged
    non_merge_df = df[~df['代號'].isin(merge_codes)]

    # Step 2: Filter data for specified merge codes and perform merge operations
    merge_df = df[df['代號'].isin(merge_codes)]
    merge_df['代號'] = target_code
    merge_df['名稱'] = target_name

    for col in merge_df.columns:
        if merge_df[col].dtype == 'object' and col not in ['代號', '名稱']:
            try:
                merge_df[col] = merge_df[col].str.replace(',', '').astype(float)
            except ValueError:
                pass  # Ignore columns that cannot be converted to float

    merged_df = merge_df.groupby(['代號', '名稱', '年月'], as_index=False).sum()

    # Step 3: Concatenate non-merged data with merged data
    final_df = pd.concat([non_merge_df, merged_df], ignore_index=True)

    return final_df

