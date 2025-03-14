import pydsd
import xarray as xr
import os
import numpy as np
import argparse
import requests
import sage_data_client

from netCDF4 import num2date, date2num
from ArgonneParsivelReader import read_adm_parsivel
from datetime import datetime, timedelta

def process_parsivel(day, radar_frequency=None, node="W09A", username="", password=""):
    """
    Process raw output from the Parsivel on the Argonne Deployable Mast.

    Parameters
    ----------
    fi: str
        The name of the file to process.    
    radar_frequency: float
        If radar moments are to be processed into a b1-level product, then the radar frequency in Hz.
        Set to None to skip processing radar moments to create an a1-level product with the PSDs and curve fits.
    node: str
        WSN for node
    username: str
        Waggle username
    password: str
        Waggle password
    """
    start = day.strftime('%Y-%m-%dT%H:%M:%SZ')
    end = (day + timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%SZ')
    print(start)
    df_files = sage_data_client.query(
            start=start,
            end=end, 
            filter={
                "name" : 'upload',
                "plugin": "registry.sagecontinuum.org/jrobrien/waggle-parsivel-io:0.25.02.04.*",
                "vsn" : node,
            }
        )
    print(df_files)
    ds_list = []
    for filename in df_files["value"]:
        response = requests.get(filename, auth=(username, password))
        if response.status_code == 200:
            # Save the file locally
            with open("temp.csv", "wb") as file:
                file.write(response.content)

        base, name = os.path.split(filename)

        my_dsd = read_adm_parsivel("temp.csv")
        print("PSD read in")
        # MRR2 Frequency is 24 Ghz
        # W-band is 95 Ghz
        if len(my_dsd.Nd["data"]) == 0:
            print("Empty file, skipping.")
            continue
        my_dsd.Nd["data"] = my_dsd.Nd["data"].filled(0)
        
        my_dsd.calculate_dsd_parameterization()
        out_ds = xr.Dataset()
        out_ds["time"] = ('time', my_dsd.time["data"])
        out_ds["bin_edges"] = ('bin_edges', my_dsd.bin_edges["data"])
        out_ds["bin_edges"].attrs = my_dsd.bin_edges
        del out_ds["bin_edges"].attrs["data"]
        
        out_ds["Nd"] = (['time', 'bins'], my_dsd.Nd["data"])
        out_ds["Nd"].attrs = my_dsd.Nd
        del out_ds["Nd"].attrs["data"]
        out_ds["num_particles"] = (['time'], my_dsd.num_particles["data"])
        out_ds["num_particles"].attrs = my_dsd.num_particles
        del out_ds["num_particles"].attrs["data"]
        out_ds["velocity"] = (['time', 'bins'], my_dsd.velocity["data"])
        out_ds["velocity"].attrs = my_dsd.velocity
        del out_ds["velocity"].attrs["data"] 
        out_ds["rain_rate"] = (['time'], my_dsd.rain_rate["data"])
        del my_dsd.rain_rate["data"]
        out_ds["rain_rate"].attrs = my_dsd.rain_rate
        if radar_frequency is not None:
            my_dsd.set_scattering_temperature_and_frequency(scattering_freq=24e6)
            my_dsd.calculate_radar_parameters()
            print("Scattering done")
            params_list = ["Zh", "Zdr", "delta_co", "Kdp", "Ai", "Adr", "D0", "Dmax", "Dm", "Nt", "Nw", "N0", "W", "mu", "Lambda",
                        "sensor_status", "error_code", "num_particles_validated", "power_supply_voltage", "sensor_head_heating_current",
                        "sensor_heating_temperature", "temperature_right_head", "temperature_left_head", "sensor_time"]
            for param in params_list:
                out_ds[param] = (['time'], my_dsd.fields[param]["data"])
                out_ds[param].attrs = my_dsd.fields[param]
                del out_ds[param].attrs["data"]
        else:
            params_list = ["D0", "Dmax", "Dm", "Nt", "Nw", "N0", "W", "mu", "Lambda",
                        "sensor_status", "error_code", "num_particles_validated", "power_supply_voltage", "sensor_head_heating_current",
                        "sensor_heating_temperature", "temperature_right_head", "temperature_left_head", "sensor_time"]
            for param in params_list:
                out_ds[param] = (['time'], my_dsd.fields[param]["data"])
                out_ds[param].attrs = my_dsd.fields[param]
                del out_ds[param].attrs["data"]
        out_ds['bins'] = (['bins'],
                        (out_ds['bin_edges'][1:].values+out_ds['bin_edges'][:-1].values)/2)
        out_ds['bins'].attrs["long_name"] = "Bin mid points"
        out_ds['bins'].attrs["units"] = "mm"
        out_ds.attrs["site"] = "Argonne Deployable Mast"
        out_ds.attrs["mentors"] = "Liz Wawrzyniak, Joseph O'Brien, Bobby Jackson, Bhupendra Raut"
        out_ds.attrs['mentor_emails'] = "ewawrzyniak@anl.gov, obrien@anl.gov, rjackson@anl.gov, braut@anl.gov"
        out_ds.attrs['mentor_institution'] = 'Argonne National Laboratory'
        out_ds.attrs['mentor_orcids'] = "0000-0003-4655-6912, 0000-0003-2518-1234"
        out_ds.attrs['contributors'] = "Scott Collis, Paytsar Muradyan, Max Grover, Matt Tuftedal"
        out_ds["time"] = out_ds["time"].astype("datetime64[s]")
        ds_list.append(out_ds)
        os.remove("temp.csv")
    out_ds = xr.concat(ds_list, dim='time').sortby("time")
    return out_ds

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog='ParsivelIngest',
        description='Parsivel data ingest')
    parser.add_argument('--date', default=datetime.now().strftime('%Y%m%d'), 
                        help='Day to process in [YYYYMMDD] format, default is today')
    parser.add_argument('--output_path', default=os.getcwd(), help='Path to output directory')
    parser.add_argument('--node', default="W09A", help='Waggle node number containing data')
    parser.add_argument('--frequency', default=None,
                         help="Radar frequency for generating radar moments." + 
                              "This will make the data level b1 instead of a1.")
    parser.add_argument('--waggle_username', default="", 
                        help='Waggle username.')
    parser.add_argument('--waggle_password', default="", 
                        help='Waggle password.')
    args = parser.parse_args()
    day = datetime.strptime(args.date, '%Y%m%d')
    username = args.waggle_username
    password = args.waggle_password
    out_ds = process_parsivel(day, args.frequency, args.node, username, password)
    
    if args.frequency is None:
        data_level = "a1"
    else:
        data_level = "b1"
    
    file_date = out_ds.time[0].dt.strftime('%Y%m%d.%H%M%S').values
    del out_ds["sensor_time"].attrs["units"]
    node = args.node
    out_file = os.path.join(args.output_path, f'ADM.parsivel.{node}.{file_date}.{data_level}.nc')
    out_ds.to_netcdf(out_file)

    













