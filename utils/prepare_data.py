import pandas as pd

def prepare_data(df_):
    df = df_.copy()
    dfs = []
    unique_kpi = df['KPI ID'].unique()
    for kpi in unique_kpi:
        df_tmp = df[df['KPI ID'] == kpi]
        df_tmp['datetime'] = pd.to_datetime(df_tmp['timestamp'], unit='s')
        df_tmp = df_tmp.set_index('datetime')
        df_tmp = df_tmp.sort_index()
        df_tmp_value = df_tmp[['value','KPI ID']].resample('1T').interpolate()
        
        if 'label' in df.columns:
            df_tmp_label = df_tmp['label'].resample('1T').ffill()
            df_tmp = pd.concat([df_tmp_value, df_tmp_label], axis=1, join='inner')
        else:
            df_tmp = df_tmp_value.to_frame()

        df_tmp.reset_index(inplace=True)
        dfs.append(df_tmp)
        
    df_upsampled = pd.concat(dfs)
    return df_upsampled