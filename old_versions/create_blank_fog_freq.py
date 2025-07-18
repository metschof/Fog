import pandas as pd

months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']

#  ICOADS data is presented in the lat/lon ranges: -90<=lat<=90 and 0<=lon<360
#  ArcGIS requires data in the lat/lon ranges: -90<=lat<=90 and -180<=lon<180
#  Longitude must be converted from one format to the other when populating the tally chart

lats = list(range(-90,90))
lons = list(range(-180, 180))
fog_freq = pd.DataFrame(columns = ['Month', 'Longitude', 'Latitude', 'fog_obs', 'total_obs', 'fog_percentage'])
i=0
for month in months:
    for lon in lons:
        for lat in lats:
            fog_freq.loc[i] = [month, lon, lat, 0, 0, 0]
            i = i + 1

fog_freq.to_csv('/data/users/mark.schofield/fog/fog_data/blank_fog_freq.csv')
print('saved')
