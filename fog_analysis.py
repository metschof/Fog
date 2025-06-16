import pandas as pd
import os

dir = '/data/users/mark.schofield/fog/fog_data/csv_files/n_atlantic'

# Create a fog_df DataFrame to record the frequency
# of each fog observation by lat, long and month
months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
lat_min = 45
lat_max = 70
lon_min = -45
lon_max = 20
lats = list(range(lat_min, lat_max + 1))
lons = list(range(lon_min, lon_max + 1))
fog_freq = pd.DataFrame(columns = ['Lat', 'Lon', 'Month', 'WW40', 'WW41', 'WW42', 'WW43', 'WW44', 'WW45', 'WW46', 'WW47', 'WW48', 'WW49', 'fog_obs', 'total_obs', 'fog_percentage' ])
i=0
for lat in lats:
    for lon in lons:
        for month in months:
            fog_freq.loc[i] = [lat, lon, month, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
            i=i+1


# Iterate over all files for a given time frame and subregion
file_list = ['ICOADS_R3.0_Rqst796986_20011115-20011231.csv']
#for file in file_list:
for file in os.listdir(dir):
    print(file)
    path = os.path.join(dir, file)
    data = pd.read_csv(path)            
            
    # Round all lats and lons to the nearest degree
    # Convert lons from 0to360 to -180to180
    data['LAT'] = data['LAT'].round(0).astype(int)
    data['LON'] = data['LON'].round(0).astype(int)
    data['LON'] = data['LON'].apply(lambda x:x-360 if x > 180 else x)


    # Create a tally chart of fog type frequencies for every lat, lon and month
    for j in range(data.shape[0]):
        lat = data.iloc[j]['LAT']
        lon = data.iloc[j]['LON']
        month = data.iloc[j]['MO']
        ww = data.iloc[j]['WW']
        col_head = 'WW' + str(ww)
        row = (lat-lat_min)*len(months)*len(lons) + (lon-lon_min)*len(months) + month - 1
#        print(lat, lon, month, ww, row)
        fog_freq.at[row, 'total_obs'] = fog_freq.loc[row]['total_obs'] + 1
        if ((ww >= 40) and (ww < 50)) :
            fog_freq.at[row, col_head] = fog_freq.loc[row][col_head] + 1
            fog_freq.at[row, 'fog_obs'] = fog_freq.loc[row]['fog_obs'] + 1

            
    # Calculate percentages
    fog_freq['fog_percentage'] = fog_freq['fog_obs'] / fog_freq['total_obs'] * 100
    fog_freq['fog_percentage'] = fog_freq['fog_percentage'].round(2)

    
# Print a frequency list of all 1 deg grid cells that have fog observed
for j in range(fog_freq.shape[0]):
    if fog_freq.loc[j]['total_obs'] > 100:
        print(fog_freq.loc[j]['Lat'], fog_freq.loc[j]['Lon'], fog_freq.loc[j]['Month'], fog_freq.loc[j]['fog_obs'], fog_freq.loc[j]['total_obs'], fog_freq.loc[j]['fog_percentage'] )

file = 'fog_freq_n_atlantic_19700101_19991231.csv'
path = os.path.join(dir, file)
fog_freq.to_csv(path)
            


        
