import pandas as pd
from zipfile import ZipFile
from typing import List

def read_table_from_zip(table_name: str, zip_path: str = "data/turkcell/RegionA.zip" ):
    with ZipFile(zip_path) as zip_file:
        with zip_file.open(table_name) as file:
            df = pd.read_csv(file, sep="\t", index_col=0)
            if "datetime" in df:
                df["datetime"] = pd.to_datetime(df["datetime"])
            return df
        


