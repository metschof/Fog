import pandas as pd
import os
import numpy as np

dir = '/data/users/mark.schofield/fog/fog_data/csv_files/global'
#dir = '/data/users/mark.schofield/fog/fog_data/csv_files/test_file'
save_dir = '/data/users/mark.schofield/fog/fog_data/csv_files/outputs'

region = 'n_atlantic'

#  ICOADS data is presented in the lat/lon ranges: -90<=lat<=90 and 0<=lon<360
#  ArcGIS requires data in the lat/lon ranges: -90<=lat<=90 and -180<=lon<180

# Latitude in range -90 <= lat <= 90
lat_min = 45
lat_max = 70

# Longitude in the range -180 <= lon < 180
lon_min = -45
lon_max = 20           # If the region spans the Pacific meridian, max < min

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

icoads_lons = [ x + 360 if x < 0 else x for x in lons]


# Create a blank fog_df DataFrame to record the frequency
# of each fog observation by lat, long and month
months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']

fog_tally = pd.DataFrame(columns = ['Month', 'Longitude', 'Latitude', 'fog_obs', 'total_obs', 'fog_percentage'])
i=0
for month in months:
    for lon in lons:
        for lat in lats:
            fog_tally.loc[i] = [month, lon, lat, 0, 0, 0.0]
            i = i + 1
            
            
# Iterate over all files for a given time frame and subregion
count = 0
for root, dirs, files in os.walk(dir):
    for file in files:
        print(count, " ", file[23:40], end="")         # Timeframe being analysed
        count += 1
        path = os.path.join(root, file)
        global_data = pd.read_csv(path)
        if count%1000 == 0:
            print(count)

        # Round all lats and lons to the nearest degree
        global_data['LAT'] = np.floor(global_data['LAT']).astype(int)
        global_data['LON'] = np.floor(global_data['LON']).astype(int)
        
        # Filter global_data to regional boundaries
        data = global_data[global_data['LON'].isin(icoads_lons) & global_data['LAT'].isin(lats)].copy()
        
        # Convert ICOADS lons from 0to360 to -180to180
        data['LON'] = data['LON'].apply(lambda x:x-360 if x > 180 else x)
    
        i=0
    
        # Create a tally chart of fog type frequencies for every lat, lon and month
        for j in range(data.shape[0]):
            # Identify the index of the row in the tally chart relevant to the row of data
            month = months[data.iloc[j]['MO']-1]
            lat = data.iloc[j]['LAT']
            lon = data.iloc[j]['LON']
            ww = data.iloc[j]['WW']
            rule1 = fog_tally['Month'] == month
            rule2 = fog_tally['Longitude'] == lon
            rule3 = fog_tally['Latitude'] == lat
            row = fog_tally.index[rule1 & rule2 & rule3].tolist()[0]

            # Increase tally in relevant row of tally chart
            fog_tally.at[row, 'total_obs'] = fog_tally.loc[row]['total_obs'] + 1
            if ((ww >= 40) and (ww < 50)) :
                fog_tally.at[row, 'fog_obs'] = fog_tally.loc[row]['fog_obs'] + 1
                i += 1

# Calculate percentages
fog_tally['fog_percentage'] = (fog_tally['fog_obs'] / fog_tally['total_obs'] * 100).fillna(0)
fog_tally['fog_percentage'] = fog_tally['fog_percentage'].round(2)
fog_tally['fog_percentage'] = np.where(fog_tally['total_obs'] < 100, -1, fog_tally['fog_percentage'])

print(fog_tally.head(10))


# Reformat analysis into new DataFrame with correct format for running through ArcGIS    
cols = ['Longitude', 'Latitude']
headers = cols + months
fog_freq2= pd.DataFrame(columns = headers)
i=0
for lon in lons:
    for lat in lats:
        fog_freq2.loc[i] = [lon, lat, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        i += 1

for month in months:
    filtered_df = fog_tally[fog_tally['Month'] == month]
    fog_freq2[month]=filtered_df['fog_percentage'].values

fog_averages = pd.DataFrame(columns = months)
for i in range(3):
    fog_averages.loc[i] = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]

for month in months:
    month_df = fog_tally[(fog_tally['Month'] == month) & (fog_tally['fog_percentage'] != -1)]
    fog_averages.loc[0, month] = month_df['fog_obs'].sum()
    fog_averages.loc[1, month] = month_df['total_obs'].sum()
    if month_df['total_obs'].sum() > 0:
        fog_averages.loc[2,month] = month_df['fog_obs'].sum()/ month_df['total_obs'].sum()*100
print(fog_averages)


# Save output file
save_file = region + '_tally_chart.csv'
path = os.path.join(save_dir, save_file)
fog_tally.to_csv(path)

save_file = region + '_monthly_percentages.csv'
path = os.path.join(save_dir, save_file)
fog_freq2.to_csv(path)

save_file = region + '_monthly_averages.csv'
path = os.path.join(save_dir, save_file)
fog_averages.to_csv(path)
            


        
