import os
import sqlite3
from typing import Dict, List

import pandas as pd


def load_csv_data(file_path: str) -> pd.DataFrame:
    """
    指定されたパスからCSVデータを読み込む関数。

    Args:
        file_path (str): 読み込むCSVファイルのパス。

    Returns:
        pd.DataFrame: 読み込んだCSVデータをDataFrameとして返す。
    """
    return pd.read_csv(file_path)


def load_excel_data(file_path: str, sheet_name: str) -> pd.DataFrame:
    """
    指定されたパスからExcelデータを読み込む関数。

    Args:
        file_path (str): 読み込むExcelファイルのパス。
        sheet_name (str): 読み込むシート名。

    Returns:
        pd.DataFrame: 読み込んだシートのデータをDataFrameとして返す。
    """
    return pd.read_excel(file_path, sheet_name=sheet_name)


def preprocess_csv_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    CSVデータの前処理を行う。計算対象が1のものを抽出し、0のものはルールに基づき処理。

    Args:
        df (pd.DataFrame): 処理前のデータ。

    Returns:
        pd.DataFrame: 前処理後のデータ。
    """
    # 計算対象が1の行のみ取得
    df_filtered = df[df["計算対象"] == 1].copy()

    # 計算対象が0の行に対するルール処理
    # 例: '出張'を大項目 '出張' にして処理
    df_zero = df[df["計算対象"] == 0].copy()
    df_zero["大項目"] = df_zero["大項目"].apply(lambda x: "出張" if "出張" in x else x)

    # 結合
    df_processed = pd.concat([df_filtered, df_zero])

    return df_processed


def preprocess_excel_data(df: pd.DataFrame, u1: bool) -> pd.DataFrame:
    """
    Excelデータの前処理を行う。U1シートまたはU2シートに基づいて適切な金額を設定し、必要な列のみ抽出。

    Args:
        df (pd.DataFrame): 処理前のExcelデータ。
        u1 (bool): U1シートならTrue, U2シートならFalse。

    Returns:
        pd.DataFrame: 前処理後のデータ。
    """
    # 計算対処のカラムがない場合は追加
    if "計算対象" not in df.columns:
        df["計算対象"] = 1
    if u1:
        # U1シートの金額は "金額 - U2 負担"
        df["金額（円）"] = df["U1金額 (円)"] - df["U2 負担"]

    else:
        # U2シートの金額は U1 負担列
        df["金額（円）"] = (
            df["U2金額 (円)"] * df["U1 比率"] / (df["U1 比率"] + df["U2 比率"])
        )  # 清算済みFlagがTrueのデータのみ
    # df_processed = df[df["清算済Flag"] == True].copy()
    df_processed = df.copy()
    # 必要な列の整理
    df_processed = df_processed[["ID", "日付", "内容", "金額（円）", "大項目", "中項目", "メモ", "計算対象"]]

    return df_processed


def convert_dates_to_string(df: pd.DataFrame, date_columns: List[str]) -> pd.DataFrame:
    """
    DataFrame内の指定された日付列を文字列形式に変換する関数。

    Args:
        df (pd.DataFrame): 処理するデータフレーム。
        date_columns (List[str]): 日付列の名前のリスト。

    Returns:
        pd.DataFrame: 日付列を文字列形式に変換したデータフレーム。
    """
    for col in date_columns:
        # df[col] = pd.to_datetime(df[col]).dt.strftime("%Y-%m-%d")
        df[col] = pd.to_datetime(df[col]).dt.date
    return df


def insert_data_to_sqlite(db_path: str, table_name: str, df: pd.DataFrame, replace: bool = False) -> None:
    """
    SQLiteデータベースにデータを挿入する関数。

    Args:
        db_path (str): dbファイルのパス。
        table_name (str): 挿入するテーブルの名前。
        df (pd.DataFrame): 挿入するデータのDataFrame。
        replace (bool): テーブルが存在する場合に置き換えるかどうか。
    """
    # SQLiteデータベースの接続を確立
    conn = sqlite3.connect(db_path)

    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS my_table (
            ID INTEGER PRIMARY KEY,            -- ユニークIDとしてプライマリキーを設定
            計算対象 INTEGER,                  -- 0 または 1
            日付 DATE,                         -- 日付
            内容 TEXT,                         -- 内容
            金額 FLOAT,                        -- 金額
            保有金融機関 TEXT,                 -- 保有金融機関
            大項目 TEXT,                       -- 大項目
            中項目 TEXT,                       -- 中項目
            メモ TEXT,                         -- メモ
            振替 TEXT                          -- 振替
        )
        """
    )
    # データをSQLiteに挿入
    if replace:
        df.to_sql(table_name, conn, if_exists="replace", index=False, method="multi", chunksize=1000)
    else:
        existing_ids = pd.read_sql_query("SELECT ID FROM my_table", conn)
        existing_ids_set = set(existing_ids["ID"])
        new_data = df[~df["ID"].isin(existing_ids_set)]
        if not new_data.empty:
            new_data.to_sql(table_name, conn, if_exists="append", index=False, method="multi", chunksize=1000)

    # データベースのコミットと接続のクローズ
    conn.commit()
    conn.close()


# CSVファイルの読み込み
csv_dir = os.path.join("/home/user/github/roommate-bill-splitting-tool/private/downloaded_csv")
csv_files = [f for f in os.listdir(csv_dir) if f.endswith(".csv")]

csv_data = pd.DataFrame()
for file in csv_files:
    df = load_csv_data(os.path.join(csv_dir, file))
    df_processed = preprocess_csv_data(df)
    csv_data = pd.concat([csv_data, df_processed])

# Excelファイルの読み込み
excel_file = os.path.join("/mnt/c/Users/user/OneDrive/共有/_bill_splitting.xlsx")
u1_data = pd.read_excel(excel_file, "U1")
u2_data = pd.read_excel(excel_file, "U2")

u1_processed = preprocess_excel_data(u1_data, u1=True)
u2_processed = preprocess_excel_data(u2_data, u1=False)

# データをまとめる
all_data = pd.concat([csv_data, u1_processed, u2_processed])
all_data = convert_dates_to_string(all_data, ["日付"])


# SQLiteデータベースの接続を確立
db_path = "financial_data.db"
insert_data_to_sqlite(db_path, "financial_data", all_data)
