import os
from multiprocessing import Pool
import pandas as pd
from collections import defaultdict
import json


# List all the csv files
csv_files = [f for f in os.listdir("Data/Raw") if f.endswith('.csv')]
path = "Data/Raw"

# Combine the files by station names
station_files = defaultdict(list)
for f in csv_files:
    station = f.split('_')[0]
    station_files[station].append(os.path.join(path, f))

def process_station(args):
    station, file_list = args

    # Combine CSVs for the station
    df_list = [pd.read_csv(f) for f in file_list]
    df = pd.concat(df_list, ignore_index=True)

    # Invalid rows skipped
    df = df[df['QC_FLAG'].isna() | (df['QC_FLAG'] == '')]
    
    # Ensure DATE is a datetime object
    df['DATE'] = pd.to_datetime(df['DATE'], errors='coerce')
    df = df.sort_values('DATE')

    # Hottest Days
    max_temp = df['TMAX'].max()
    max_rows = df[df['TMAX'] == max_temp]
    max_dates = list(max_rows['DATE'].dt.date)

    # Coldest Days
    min_temp = df['TMIN'].min()
    min_rows = df[df['TMIN'] == min_temp]
    min_dates = list(min_rows['DATE'].dt.date)

    # Wettest Days
    wettest = df['PRCP'].max()
    wettest_rows = df[df['PRCP'] == wettest]
    wettest_dates = list(wettest_rows['DATE'].dt.date)

    # Heatwave
    df['is_heat'] = df['TMAX'] >= 90
    df['group'] = (df['is_heat'] != df['is_heat'].shift()).cumsum()

    heat_groups = df[df['is_heat']].groupby('group')

    max_len = 0
    longest_wave = None

    for _, group in heat_groups:
        if len(group) >= 3 and len(group) > max_len:  # Heatwave: 3+ consecutive days
            max_len = len(group)
            longest_wave = group

    heatwave_summary = None
    if longest_wave is not None:
        heatwave_summary = {
            'start': longest_wave['DATE'].iloc[0].date(),
            'end': longest_wave['DATE'].iloc[-1].date(),
            'length': max_len
        }

    # Yearly Stats
    df['year'] = df['DATE'].dt.year
    yearly_stats = []

    for year, group in df.groupby('year'):
        stats = group[['TMAX', 'TMIN', 'PRCP', 'SNOW', 'SNWD']].agg(['mean', 'median', 'std', 'min', 'max'])

        yearly_stats.append({
            'year': year,
            'TMAX': stats['TMAX'].to_dict(),
            'TMIN': stats['TMIN'].to_dict(),
            'PRCP': stats['PRCP'].to_dict(),
            'SNOW': stats['SNOW'].to_dict(),
            'SNWD': stats['SNWD'].to_dict()
        })

    print(f"Station {station}: Max {max_temp}, Min {min_temp}, PRCP {wettest}, Heatwave Length {heatwave_summary['length'] if heatwave_summary else 0}")

    return {
        'station': station,
        'max_temp': max_temp,
        'max_dates': max_dates,
        'min_temp': min_temp,
        'min_dates': min_dates,
        'wettest': wettest,
        'wettest_dates': wettest_dates,
        'heatwave': heatwave_summary,
    },yearly_stats


def main():
    # Group files by station
    from collections import defaultdict
    station_files = defaultdict(list)
    for f in csv_files:
        station = f.split('_')[0]
        station_files[station].append(os.path.join(path, f))

    # Convert to list of tuples for multiprocessing
    station_args = list(station_files.items())

    # Process each station in parallel
    with Pool(processes=min(len(station_args), os.cpu_count())) as pool:
        results = pool.map(process_station, station_args)
        print(results)
    
    global_max_temp = float('-inf')
    global_max_info = None

    global_min_temp = float('inf')
    global_min_info = None

    for station_result, _ in results:
        if station_result['max_temp'] > global_max_temp:
            global_max_temp = station_result['max_temp']
            global_max_info = {
                'station': station_result['station'],
                'temp': station_result['max_temp'],
                'dates': station_result['max_dates']
            }

        if station_result['min_temp'] < global_min_temp:
            global_min_temp = station_result['min_temp']
            global_min_info = {
                'station': station_result['station'],
                'temp': station_result['min_temp'],
                'dates': station_result['min_dates']
            }

    print("Global Hottest Day(s)")
    print(global_max_info)

    print("Global Coldest Day(s)")
    print(global_min_info)

    summary = {
        "results": results,
        "global_coldest_day": global_min_info,
        "global_hottest_day": global_max_info
    }

    with open("Data/Output/summary.json", "w") as f:
        json.dump(summary, f, indent=4, default=str)
    
if __name__ == '__main__':
    main()

