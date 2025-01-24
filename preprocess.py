import pandas as pd 
from utils import read_table_from_zip


def prepare_dataset(train_test_flag):
    if train_test_flag == "train":
        rl_kpis = read_table_from_zip("rl-kpis.tsv")
        df_weather = pd.read_csv("data/turkcell/aggregated_weather_data_for_5_nearest_station_train.csv")
    else:
        rl_kpis =  pd.read_csv("/root/projects/ci_anomaly_detection/data/turkcell/rl_kpis_test.csv")
        df_weather = pd.read_csv("data/turkcell/aggregated_weather_data_for_5_nearest_station_test.csv")
    
    rl_kpis.rlf.replace({False:0,True:1},inplace=True)

    rl_kpis['datetime'] = pd.to_datetime(rl_kpis['datetime'])
    rl_kpis['datetime'] = rl_kpis['datetime'].dt.date

    rl_kpis['datetime'] = rl_kpis['datetime'].astype(str)
    df_weather['datetime'] = df_weather['datetime'].astype(str)

    df_merged = pd.merge(left = rl_kpis, right = df_weather, how = "left", on = ['site_id','datetime'])
    df_merged.drop(columns = ['pressure_mean'], inplace=True)


    df_merged['datetime'] = pd.to_datetime(df_merged['datetime'])

    cols_to_fill = ['temp_mean', 'wind_dir_mean', 'wind_speed_mean', 
        'humidity_mean', 'precipitation_mean', 'precipitation_coeff_mean',
        'temp_max_day1', 'temp_min_day1','humidity_max_day1', 
        'humidity_min_day1', 'wind_dir_day1','wind_speed_day1', 
        'temp_max_day2', 'temp_min_day2','humidity_max_day2', 
        'humidity_min_day2', 'wind_dir_day2','wind_speed_day2', 
        'temp_max_day3', 'temp_min_day3','humidity_max_day3', 
        'humidity_min_day3', 'wind_dir_day3','wind_speed_day3', 
        'temp_max_day4', 'temp_min_day4','humidity_max_day4', 
        'humidity_min_day4', 'wind_dir_day4','wind_speed_day4', 
        'temp_max_day5', 'temp_min_day5','humidity_max_day5', 
        'humidity_min_day5', 'wind_dir_day5','wind_speed_day5']

    #TODO replace that
    for col in cols_to_fill:
        df_merged[col] = df_merged.groupby(['site_id','mlid'])[col].transform(
            lambda group: group.fillna(method='bfill').fillna(method='ffill')
        )

    # TODO replace that
    for col in cols_to_fill:     
        df_merged[col].fillna(df_merged[col].mean(),inplace=True)

    df_merged['capacity'] = df_merged['capacity'].astype(float)

    df_merged['freq_band'].fillna(value="f0", inplace=True)
    df_merged['capacity'].fillna((df_merged['capacity'].mean()), inplace=True)

    # nans = pd.DataFrame(df_merged.isna().sum(),columns=['count'])
    # nans.reset_index(inplace=True)
    # nans[nans['count']>0].to_csv("data/turkcell/result_nans.csv")

    result_df = df_merged.copy()
    cols_to_lag = ['temp_mean', 'wind_dir_mean', 'wind_speed_mean', 'humidity_mean', 'precipitation_mean', 
                'precipitation_coeff_mean','bbe','error_second','severaly_error_second','error_second',	
                'unavail_second']

    for i in [1,2,3]:
        time_lag_df = df_merged[['site_id','mlid','datetime']+cols_to_lag].groupby(['site_id','mlid']).shift(i)
        time_lag_df.drop(columns=['datetime'], inplace=True)
        time_lag_columns = []
        for col in  time_lag_df.columns.values:
            time_lag_columns.append(f"{col}_{i}")
        time_lag_df.columns =  time_lag_columns
        result_df = pd.concat([result_df,time_lag_df],axis=1)
    

    print(f"Finished {train_test_flag}")
    result_df.to_csv(f"data/turkcell/final_preprocessed_df_{train_test_flag}.csv", index=False)

# prepare_dataset("train")
prepare_dataset("test")