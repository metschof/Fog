import pandas as pd
import os
import numpy as np

dir = '/data/users/mark.schofield/fog/fog_data/csv_files/global'
# dir = '/data/users/mark.schofield/fog/fog_data/csv_files/global'
save_dir = '/data/users/mark.schofield/fog/fog_data/csv_files/outputs'

region = 's_africa'
weather = 'fog'            # Choose either 'fog' or 'mist'

#  ICOADS data is presented in the lat/lon ranges: -90<=lat<=90 and 0<=lon<360
#  ArcGIS requires data in the lat/lon ranges: -90<=lat<=90 and -180<=lon<180

# Latitude in range -90 <= lat <= 90
lat_min = -47
lat_max = -25

# Longitude in the range -180 <= lon < 180
lon_min = -2
lon_max = 41           # If the region spans the Pacific meridian, max < min

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
months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun',
          'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
seasons = ['djf', 'mam', 'jja', 'son']
season_months = [[12, 1, 2], [3, 4, 5], [6, 7, 8], [9, 10, 11]]

if weather == 'fog':
    wx_codes = [11, 12, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49]
else:
    wx_codes = [10]

fog_tally = pd.DataFrame(columns=['Month', 'Latitude', 'Longitude',
                                  'fog_obs', 'total_obs', 'fog_percentage'])
season_tally = pd.DataFrame(columns=['Season', 'Latitude', 'Longitude',
                                     'fog_obs', 'total_obs', 'fog_percentage'])
i = 0
for month in months:
    for lat in lats:
        for lon in lons:
            fog_tally.loc[i] = [month, lat, lon, 0, 0, 0.0]
            i += 1
i = 0
for season in seasons:
    for lat in lats:
        for lon in lons:
            season_tally.loc[i] = [season, lat, lon, 0, 0, 0.0]
            i += 1

# print(season_tally.head(30))

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
        for season in list(range(4)):
            for lat in lats:
                for lon in lons:
                    tally_all_obs_season = len(data[(data['MO'].
                                                     isin(season_months[season]))
                                                    & (data['LAT'] == lat)
                                                    & (data['LON'] == lon)])
                    tally_fog_obs_season = len(data[(data['MO'].
                                                     isin(season_months[season]))
                                                    & (data['LAT'] == lat)
                                                    & (data['LON'] == lon)
                                                    & (data['WW'].isin(wx_codes))])
                    pos_lat = lats.index(lat)
                    pos_lon = lons.index(lon)
                    season_tally.at[row, 'total_obs'] = season_tally.loc[row]['total_obs']
                    + tally_all_obs_season
                    season_tally.at[row, 'fog_obs'] = (season_tally.loc[row]['fog_obs'])
                    + tally_fog_obs_season
                    row += 1

# Calculate percentages
season_tally['fog_percentage'] = (season_tally['fog_obs'] / season_tally['total_obs']
                                  * 100).fillna(0)
season_tally['fog_percentage'] = season_tally['fog_percentage'].round(2)
season_tally['fog_percentage'] = np.where(season_tally['total_obs'] < 100, -1,
                                          season_tally['fog_percentage'])


# Reformat analysis into new DataFrame
# with correct format for running through ArcGIS
cols = ['Latitude', 'Longitude']
headers = cols + months
fog_freq2_season = pd.DataFrame(columns=(cols + seasons))
i = 0
for lat in lats:
    for lon in lons:
        fog_freq2_season.loc[i] = [lat, lon, 0, 0, 0, 0]
        i += 1

for season in seasons:
    filtered_df_season = season_tally[season_tally['Season'] == season]
    fog_freq2_season[season] = filtered_df_season['fog_percentage'].values
print(fog_freq2_season.head(20))

fog_averages_seasons = pd.DataFrame(columns=seasons)

for i in range(3):
    fog_averages_seasons.loc[i] = [0.0, 0.0, 0.0, 0.0]

for season in seasons:
    season_df = season_tally[(season_tally['Season'] == season)
                             & (season_tally['fog_percentage'] != -1)]
    fog_averages_seasons.loc[0, season] = season_df['fog_obs'].sum()
    fog_averages_seasons.loc[1, season] = season_df['total_obs'].sum()
    if season_df['total_obs'].sum() > 0:
        fog_averages_seasons.loc[2, season] = season_df['fog_obs'].sum() / season_df['total_obs'].sum() * 100
print(fog_averages_seasons)


# Save output file
save_file = region + '-' + weather + '_seasonal_tally.csv'
path = os.path.join(save_dir, save_file)
season_tally.to_csv(path)

save_file = region + '_' + weather + '_seasonal_percentages.csv'
path = os.path.join(save_dir, save_file)
fog_freq2_season.to_csv(path)

save_file = region + '_' + weather + '_seasonal_averages.csv'
path = os.path.join(save_dir, save_file)
fog_averages_seasons.to_csv(path)
