import pandas as pd
from utils import read_table_from_zip


def create_weather_dataset(train_test_flag):
    if train_test_flag == "train":
        closest_ws = pd.read_csv("data/turkcell/5_closest_ws_train.csv")
        met_real = read_table_from_zip("met-real.tsv")
        met_forecast = read_table_from_zip("met-forecast.tsv")
    else:
        closest_ws = pd.read_csv("data/turkcell/5_closest_ws_test.csv")
        met_real = pd.read_csv("data/turkcell/met_real_test.csv")
        met_forecast = pd.read_csv("data/turkcell/met_forecast_test.csv")

    if train_test_flag == "train":
        met_real['pressure'] = met_real['pressure'].str.replace(",", "").astype(float)
        met_real['pressure_sea_level']= met_real['pressure_sea_level'].str.replace(",", "").astype(float)

    met_real_agg_by_day = met_real.groupby(["station_no","measured_date"],as_index=False).agg({"temp":"mean",
                                                                                            "wind_dir":"mean",
                                                                                            "wind_speed":"mean",
                                                                                            "humidity":"mean",
                                                                                            "precipitation":"mean",
                                                                                            "precipitation_coeff":"mean",
                                                                                            "pressure":"mean",
                                                                                            })


    # .agg({"temp":["min", "mean", "max"],
    #                                                                      "wind_dir": ["min", "mean", "max"],
    #                                                                      "wind_speed": ["min", "mean", "max"],
    #                                                                      "humidity": ["min", "mean", "max"],
    #                                                                      "precipitation": ["min", "mean", "max"],
    #                                                                      "precipitation_coeff": ["min", "mean", "max"],
    #                                                                      "pressure": ["min", "mean", "max"],
    #                                                                      })


    # met_real_agg_by_day.columns = ['_'.join(col).strip() for col in met_real_agg_by_day.columns.values]
    # met_real_agg_by_day.rename(columns={'station_no_':"station_no","measured_date_":"datetime"},inplace=True)

    met_real_agg_by_day.columns = ['station_no','datetime','temp_mean','wind_dir_mean','wind_speed_mean',
                                'humidity_mean','precipitation_mean','precipitation_coeff_mean','pressure_mean']

    met_real_agg_by_day['datetime'] = pd.to_datetime(met_real_agg_by_day['datetime'])
    met_real_agg_by_day['datetime'] = met_real_agg_by_day['datetime'].dt.date


    met_forecast_cols = ['temp_max_day1',  'temp_min_day1', 'humidity_max_day1', 'humidity_min_day1',  'wind_dir_day1', 
                        'wind_speed_day1',  'temp_max_day2',  'temp_min_day2',  'humidity_max_day2', 'humidity_min_day2', 
                        'wind_dir_day2', 'wind_speed_day2', 'temp_max_day3', 'temp_min_day3', 'humidity_max_day3','humidity_min_day3', 
                        'wind_dir_day3', 'wind_speed_day3', 'temp_max_day4', 'temp_min_day4', 'humidity_max_day4','humidity_min_day4', 
                        'wind_dir_day4',  'wind_speed_day4', 'temp_max_day5', 'temp_min_day5', 'humidity_max_day5',
                        'humidity_min_day5', 'wind_dir_day5', 'wind_speed_day5']

    cols_to_aggregate = ['temp_mean', 'wind_dir_mean' ,'wind_speed_mean',
                        'humidity_mean', 'precipitation_mean','precipitation_coeff_mean', 'pressure_mean']

    met_forecast['datetime'] = pd.to_datetime(met_forecast['datetime'])
    met_forecast['datetime'] = met_forecast['datetime'].dt.date
    met_forecast_groupby = met_forecast.groupby(['station_no','datetime'],as_index=False)[met_forecast_cols].mean()
    df_weather_merged = met_real_agg_by_day.merge(met_forecast_groupby, on = ["station_no", "datetime"], how ="outer")

    def aggregate_weather_for_nearest_stations(row: pd.DataFrame, df_weather: pd.DataFrame, closest_ws_columns, agg_columns ):
        """return aggregated weather data for n nearest station with rl"""
        station_cols = [col for col in closest_ws_columns if col.startswith('closest')]
        stations = [row[col] for col in station_cols]
        weather_data = df_weather[df_weather['station_no'].isin(stations)]
        res = weather_data.groupby(['datetime'],as_index=False)[agg_columns].mean()
        res['site_id'] = row['index']
        return res


    aggregated_data = pd.DataFrame()
    for i in range(len(closest_ws)): 
        res = aggregate_weather_for_nearest_stations(closest_ws.iloc[i], df_weather_merged, closest_ws.columns, cols_to_aggregate+ met_forecast_cols)
        aggregated_data = pd.concat([aggregated_data,res])

    print(f"Finished {train_test_flag}")
    aggregated_data.to_csv(f"data/turkcell/aggregated_weather_data_for_5_nearest_station_{train_test_flag}.csv", index=False)

create_weather_dataset("train")
create_weather_dataset("test")