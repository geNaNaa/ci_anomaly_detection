
import pandas as pd
from utils import read_table_from_zip
from typing import List

def get_top_n_stations(row, n=5) -> List:
    """return n 5 closest weather station for rl"""
    closest = row.nsmallest(n)  
    return list(closest.index)


def create_distance_csv(train_test_flag: str):
    if train_test_flag == 'train':
        df_distance = read_table_from_zip("distances.tsv")
        df_distance.reset_index(inplace=True)
    else: 
        df_distance = pd.read_csv("data/turkcell/distances_test.csv")
        df_distance.rename(columns={"Unnamed: 0":"index"},inplace=True)

    df_filtered_rows = df_distance[df_distance["index"].str.startswith("RL")]
    columns_to_keep = ["index"] + [col for col in df_distance.columns if col.startswith("WS")]
    df_filtered = df_filtered_rows[columns_to_keep].reset_index(drop=True)
    df_filtered.set_index('index',inplace=True)

    n_closest = 5

    result = pd.DataFrame(index=df_filtered.index)  

    for i in range(n_closest):
        result[f"closest_station_{i+1}"] = df_filtered.apply(
            lambda row: get_top_n_stations(row, n=n_closest)[i] if i < len(row) else None, axis=1
        )

    print(f"Finished {train_test_flag}")
    result.to_csv(f"data/turkcell/5_closest_ws_{train_test_flag}.csv")


create_distance_csv('train')
create_distance_csv('test')
