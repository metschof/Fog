import pandas as pd
import os
import numpy as np

dir = '/data/users/mark.schofield/fog/fog_data/csv_files/global'
# dir = '/data/users/mark.schofield/fog/fog_data/csv_files/test_file'
save_dir = '/data/users/mark.schofield/fog/fog_data/csv_files/outputs'

frequency = 'Season'        # Choose either 'Month' or 'Season'
region = 'global'
weather = 'fog'            # Choose either 'fog', 'shallow_fog' or 'mist'

#  ICOADS data is presented in the lat/lon ranges: -90<=lat<=90 and 0<=lon<360
#  ArcGIS requires data in the lat/lon ranges: -90<=lat<=90 and -180<=lon<180

# Latitude in range -90 <= lat <= 90
lat_min = -90
lat_max = 90

# Longitude in the range -180 <= lon < 180
lon_min = -180
lon_max = 180           # If the region spans the Pacific meridian, max < min

if lon_min > 180:
    lon_min -= 360       # ensures lon is in the right range
if lon_max > 180:
    lon_max -= 360       # ensures lat is in the right range

lats = list(range(lat_min, lat_max + 1))
# To overcome boundary issue at lon -180,180
if lon_min > lon_max:
    lons = list(range(lon_min, 180+1)) + list(range(-180+1, lon_max+1))
else:
    lons = list(range(lon_min, lon_max + 1))

icoads_lons = [x + 360 if x < 0 else x for x in lons]

# Create a blank fog_df DataFrame to record the frequency
# of each fog observation by lat, long and month
if frequency == 'Month':
    time_units = ['jan', 'feb', 'mar', 'apr', 'may', 'jun',
                  'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
else:
    time_units = ['djf', 'mam', 'jja', 'son']
season_months = [[12, 1, 2], [3, 4, 5], [6, 7, 8], [9, 10, 11]]

if weather == 'fog':
    wx_codes = [40, 41, 42, 43, 44, 45, 46, 47, 48, 49]
elif weather == 'shallow_fog':
    wx_codes = [11, 12]
else:
    wx_codes = [10]

tally = pd.DataFrame(columns=[frequency, 'Latitude', 'Longitude',
                              'fog_obs', 'total_obs', 'fog_percentage'])

i = 0

for time_unit in time_units:
    for lat in lats:
        for lon in lons:
            tally.loc[i] = [time_unit, lat, lon, 0, 0, 0.0]
            i += 1

count = 0
# Iterate over all files for a given time frame and subregion
for root, dirs, files in os.walk(dir):
    for file in files:
        print(count, file[23:40])         # Timeframe being analysed
        count += 1
        path = os.path.join(root, file)
        global_data = pd.read_csv(path)

        # Round all lats and lons to the nearest degree
        global_data['LAT'] = np.floor(global_data['LAT']).astype(int)
        global_data['LON'] = np.floor(global_data['LON']).astype(int)

        # Filter global_data to regional boundaries
        data = global_data[global_data['LON'].isin(icoads_lons)
                           & global_data['LAT'].isin(lats)].copy()

        # Convert ICOADS lons from 0to360 to -180to180
        data['LON'] = data['LON'].apply(lambda x: x-360 if x > 180 else x)

        row = 0
        for unit in list(range(len(time_units))):
            for lat in lats:
                for lon in lons:
                    condition1 = (data['LAT'] == lat)
                    condition2 = (data['LON'] == lon)
                    if frequency == 'Month':
                        condition3 = (data['MO'] == unit+1)
                    else:
                        condition3 = (data['MO'].isin(season_months[unit]))
                    condition4 = (data['WW'].isin(wx_codes))
                    tally_all_obs = len(data[condition1 & condition2 & condition3])
                    tally_fog_obs = len(data[condition1 & condition2 & condition3 & condition4])
                    pos_lat = lats.index(lat)
                    pos_lon = lons.index(lon)
                    tally.at[row, 'total_obs'] = tally.loc[row]['total_obs'] + tally_all_obs
                    tally.at[row, 'fog_obs'] = (tally.loc[row]['fog_obs']) + tally_fog_obs
                    row += 1


# Calculate percentages
tally['fog_percentage'] = (tally['fog_obs'] / tally['total_obs']
                           * 100).fillna(0)
tally['fog_percentage'] = tally['fog_percentage'].round(2)
if frequency == 'Season':
    threshold = 100
else:
    threshold = 25
tally['fog_percentage'] = np.where(tally['total_obs'] < threshold, -1,
                                   tally['fog_percentage'])


# Reformat analysis into new DataFrame
# with correct format for running through ArcGIS
cols = ['Latitude', 'Longitude']
headers = cols + time_units
fog_freq2 = pd.DataFrame(columns=(headers))
i = 0
for lat in lats:
    for lon in lons:
        fog_freq2.loc[i] = [lat, lon] + [0 for _ in range(len(time_units))]
        i += 1

for time_unit in time_units:
    filtered_df = tally[tally[frequency] == time_unit]
    fog_freq2[time_unit] = filtered_df['fog_percentage'].values

fog_averages = pd.DataFrame(columns=time_units)
for i in range(3):
    fog_averages.loc[i] = [0.0 for _ in range(len(time_units))]

for time_unit in time_units:
    df = tally[(tally[frequency] == time_unit)
               & (tally['fog_percentage'] != -1)]
    fog_averages.loc[0, time_unit] = df['fog_obs'].sum()
    fog_averages.loc[1, time_unit] = df['total_obs'].sum()
    if df['total_obs'].sum() > 0:
        fog_averages.loc[2, time_unit] = df['fog_obs'].sum() / df['total_obs'].sum() * 100
print(fog_averages)


# Save output file
save_file = region + '-' + weather + '_' + frequency + '_tally.csv'
path = os.path.join(save_dir, save_file)
tally.to_csv(path)

save_file = region + '_' + weather + '_' + frequency + '_percentages.csv'
path = os.path.join(save_dir, save_file)
fog_freq2.to_csv(path)

save_file = region + '_' + weather + '_' + frequency + '_averages.csv'
path = os.path.join(save_dir, save_file)
fog_averages.to_csv(path)
