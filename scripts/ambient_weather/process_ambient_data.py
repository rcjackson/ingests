from ambient_api.ambientapi import AmbientAPI
import numpy as np
from datetime import datetime
import time
import pandas as pd
import xarray as xr
import os
from pathlib import Path

print(os.getenv('AMBIENT_ENDPOINT'))

# Access the Ambient weather API and get the devices available
api = AmbientAPI()
devices = api.get_devices()

attrs_dict = {'tempf':{'standard_name': 'Temperature',
                       'units': 'degF'},
              'dewPoint': {'standard_name': 'Dewpoint Temperature',
                           'units': 'degF'}}

variable_mapping = {'tempf':'outdoor_temperature',
                    'dewPoint':'outdoor_dewpoint',
                    'date':'time'}


def process_station(device, attrs=attrs_dict, variable_mapping=variable_mapping):
    
    current_date = datetime.utcnow()
    # Read in the station data
    data = device.get_data(end_date = current_date)
    
    meta = device.info
    
    # Read into a pandas dataframe
    df = pd.DataFrame(data)
    
    # Format the times properly
    df['date'] = pd.to_datetime(df.date).astype('datetime64[ns]')

    # Sort the values and set the index to be the date
    df = df.sort_values('date')
    df = df.set_index('date')

    ds = df.to_xarray()

    # Add associated metadata
    for variable in attrs.keys():
        ds[variable].attrs = attrs[variable]
    
    # Rename the variables
    ds = ds.rename(variable_mapping)
        
    # Reshape the data
    ds = ds.expand_dims('station')
    ds['station'] = [meta['name']]
    ds['latitude'] = meta['coords']['coords']['lat']
    ds['longitude'] = meta['coords']['coords']['lon']
    
    ds = ds.sel(time=f"{current_date.year}-{current_date.month}-{current_date.day}")
    
    return ds

# Loop through for each device and retrieve the data, waiting for the API to clean up first
dsets = []
for device in devices:
    try:
        print(device)
        ds = process_station(device)
        site = f'{ds.station.values[0]}'
        time_label = pd.to_datetime(ds.time.values[0]).strftime(f'%Y/%m/%d/{site}.a1.%Y%m%d.000000.nc')
        full_path = Path(f'../../data/surface-meteorology/{time_label}')
        if not os.path.exists(full_path.parent):
            os.makedirs(full_path.parent)
        ds.to_netcdf(full_path)
    except:
        pass
    time.sleep(20)
