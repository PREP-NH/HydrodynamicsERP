import os
import json
import xarray as xr
import cartopy.crs as ccrs
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
from shiny import ui, render, App
# from shiny_plot_tools import ui_plot, plot_tools
import contextily as ctx
import matplotlib.colors as colors


def reformat_datetime(datetime_str):
    # Parse the datetime string to a datetime object
    truncated_str = datetime_str[:26]
    dt_obj = datetime.strptime(truncated_str, "%Y-%m-%dT%H:%M:%S.%f")

    # Format the datetime object to the desired string format
    formatted_str = dt_obj.strftime("%Y-%m-%d %H:%M")
    return formatted_str

def make_map(var):
    # Check if the file already exists
    if os.path.exists('/mnt/shiny/hydrodynamics2/maps/'):
        # Production environment
        base_path = '/mnt/shiny/hydrodynamics2/maps/'
    else:
        # Testing environment
        base_path = './bedstressshiny2/maps/'

    mapfile = base_path + 'my_' + str(var) + '.png'
    data_file = base_path + 'map_data_' + str(var) + '.txt'

    # Check if both the map file and the vector_lengths_range file exist
    if os.path.exists(mapfile) and os.path.exists(data_file):
        print("Map and vector lengths range files already exist.")
        return mapfile, data_file
    # Rest of your code for generating the map if the file does not exist
    try:
        beadshearf = xr.open_dataset('/mnt/streamlit/data/bedstress_new.nc')
    except FileNotFoundError as e:
        beadshearf = xr.open_dataset('./bedstress_new.nc')

    # [Code for processing and visualizing data]

    # map_projection = ccrs.LambertConformal(central_lon, central_lat)
    print("var: " + str(var))
    min_lon = -70.92946853
    max_lon = -70.8214239
    min_lat = 43.02604872
    max_lat = 43.10610448
    # m = Basemap(projection="merc", llcrnrlon=min_lon, llcrnrlat=min_lat,
    #             urcrnrlon=max_lon, urcrnrlat=max_lat, resolution='c')

    fig = plt.figure(figsize=(15, 8))
    # ax = fig.add_subplot(projection=ccrs.LambertConformal(central_lon, central_lat))

    ax = plt.axes(projection=ccrs.PlateCarree())
    # and extent
    ax.set_extent([min_lon, max_lon, min_lat, max_lat],
                  ccrs.PlateCarree())
    ctx.add_basemap(ax, crs=ccrs.PlateCarree(), source=ctx.providers.OpenStreetMap.Mapnik)

    lon_a = beadshearf.lon.values
    lat_a = beadshearf.lat.values
    # lon_a_sub = lon_a[::5, ::10]
    # lat_a_sub = lat_a[::5, ::10]


    if var == 'bsdur':
        cf = beadshearf.bsbdur.values  # .plot.pcolormesh(
    elif var == 'bsmean':
        cf = beadshearf.bsmean.values
    elif var == 'bsrms':
        cf = beadshearf.bsrms.values
    elif var == 'bsonethird':
        cf = beadshearf.bsonethird.values
    elif var == 'bsonetenth':
        cf = beadshearf.bsonetenth.values
    elif var == 'bsfrbdur':
        cf = beadshearf.bsfrbdur.values
    minval = np.nanmin(cf)
    maxval = np.nanmax(cf)

    norm = colors.PowerNorm(clip=False, gamma=0.5, vmin=minval, vmax=maxval, )  # (vmin = -28, vmax =1,

    # ot = beadshearf.bsbdur

    # transform=ccrs.LambertConformal(central_lon, central_lat), ax=ax2)
    cmap = plt.cm.get_cmap('inferno', 20)
    cmap.set_bad(alpha=0.0)
    cfvector_lengths_range = (np.nanmin(cf), np.nanmax(cf))

    waterdepthmap = ax.pcolormesh(lon_a, lat_a, cf, cmap=cmap, norm=norm,
                                  transform=ccrs.PlateCarree(), alpha=0.7)  # ,
    ax_pos = ax.get_position()

    cbar = plt.colorbar(waterdepthmap, ax=ax, pad=0.1)
    cbar.ax.set_position([ax_pos.x1 + 0.01, ax_pos.y0, 0.03, ax_pos.height])

    # Customize tick labels for 'bsonethird' and 'bsonetenth'

    # Customize tick labels for 'bsonethird' and 'bsonetenth'
    if var in ['bsonethird', 'bsonetenth']:
        # Get current ticks
        ticks = cbar.get_ticks()

        # Add a new tick for the critical bed stress value
        critical_stress_tick = 0.35
        if critical_stress_tick not in ticks:
            ticks = np.append(ticks, critical_stress_tick)

        # Sort the ticks
        ticks.sort()

        # Create custom tick labels
        tick_labels = [f"{tick:.2f}" for tick in ticks]
        if critical_stress_tick in ticks:
            index = np.where(ticks == critical_stress_tick)[0][0]
            tick_labels[index] = "critical bed stress >=0.35 N/m\u00B2"

        # Replace the top tick label with '>=1' if the top value is 1
        if ticks[-1] == 1:
            tick_labels[-1] = ">=1"

        # Set the ticks and custom labels
        cbar.set_ticks(ticks)
        cbar.set_ticklabels(tick_labels)

    if var == 'bsdur':
        cbar.ax.set_ylabel('Duration in hours that bed shear stress exceeds critical value of 0.35 Newtons per meter squared over a 35 day period (hours)')
    elif var == 'bsmean':
        cbar.ax.set_ylabel('Average bed shear stress over a 30 day period in Newtons per meter squared')
    elif var == 'bsrms':
        cbar.ax.set_ylabel('Root mean square (RMS) of bed shear stress over a 30 day period in Newtons per meter squared')
    elif var == 'bsonethird':
        cbar.ax.set_ylabel('Average of the highest 1/3 bed shear stress valus a 30 day period in Newtons per meter squared')
    elif var == 'bsonetenth':
        cbar.ax.set_ylabel('Average of the highest 1/10 bed shear stress valus a 30 day period in Newtons per meter squared')
    elif var == 'bsfrbdur':
        cbar.ax.set_ylabel('Fraction time in hours that bed shear stress exceeds critical value of 0.35 Newtons per meter squared over a 35 day period')


    map_data = {
        'min': str(minval),
        'max': str(maxval),
    }
    with open(data_file, 'w') as file:
        json.dump(map_data, file)
    print("Map data saved:", data_file)

    mapfile = base_path + 'my_'+ str(var) + '.png'
    fig.savefig(mapfile)  # Saves as PNG file

    print("done")

    # At the end, save the new map



    print("Map generated and saved:", mapfile)
    return mapfile, data_file

make_map('bsdur')
make_map('bsmean')
make_map('bsrms')
make_map('bsonethird')
make_map('bsonetenth')
make_map('bsfrbdur')
