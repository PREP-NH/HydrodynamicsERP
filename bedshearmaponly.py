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
from matplotlib.font_manager import FontProperties


def reformat_datetime(datetime_str):
    # Parse the datetime string to a datetime object
    truncated_str = datetime_str[:26]
    dt_obj = datetime.strptime(truncated_str, "%Y-%m-%dT%H:%M:%S.%f")

    # Format the datetime object to the desired string format
    formatted_str = dt_obj.strftime("%Y-%m-%d %H:%M")
    return formatted_str

def make_map(time_slide):
    # Check if the file already exists
    if os.path.exists('/mnt/shiny/hydrodynamics/maps/'):
        # Production environment
        base_path = '/mnt/shiny/hydrodynamics/maps/'
    else:
        # Testing environment
        base_path = './maps/'

    mapfile = base_path + 'my_map' + str(time_slide) + '.png'
    data_file = base_path + 'map_data_' + str(time_slide) + '.txt'

    # Check if both the map file and the vector_lengths_range file exist
    if os.path.exists(mapfile) and os.path.exists(data_file):
        print("Map and vector lengths range files already exist.")
        return mapfile, data_file
    # Rest of your code for generating the map if the file does not exist
    try:
        beadshearf = xr.open_dataset('/mnt/streamlit/data/gbe_0001_his_vars_hays_5days_spring_tide_whole_gb.nc')
    except FileNotFoundError as e:
        beadshearf = xr.open_dataset('./gbe_0001_his_vars_hays_5days_spring_tide_whole_gb.nc')

    # [Code for processing and visualizing data]

    # map_projection = ccrs.LambertConformal(central_lon, central_lat)
    print("time slide: " + str(time_slide))
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

    lon_rho_a = beadshearf.lon_rho.values
    lat_rho_a = beadshearf.lat_rho.values
    lon_rho_a_sub = lon_rho_a[::5, ::10]
    lat_rho_a_sub = lat_rho_a[::5, ::10]

    u = beadshearf.ubar_eastward.isel(ocean_time=time_slide).values
    v = beadshearf.vbar_northward.isel(ocean_time=time_slide).values
    # u = beadshearf['ubar_eastward'].isel(ocean_time=time_slide)
    # v = beadshearf['vbar_northward'].isel(ocean_time=time_slide)
    u_sub = u[::5, ::10]
    v_sub = v[::5, ::10]

    # -m streamlit run

    # u = beadshearf['ubar_eastward'].isel(ocean_time=time_slide)[::10].values
    # v = beadshearf['vbar_northward'].isel(ocean_time=time_slide)[::10].values
    vmin, vmax = -5, 2
    # norm = colors.CenteredNorm(vcenter=-18)
    norm = colors.PowerNorm(clip=False, gamma=0.5, vmin=-1.5, vmax=2, )  # (vmin = -28, vmax =1,
    cf = beadshearf.zeta.isel(ocean_time=time_slide).values  # .plot.pcolormesh(
    cf = cf + 24
    cfvector_lengths_range = (np.nanmin(cf), np.nanmax(cf))
    cf[cf > 2.0] = np.nan
    cf = np.ma.masked_invalid(cf)

    ot = beadshearf.ocean_time.data[time_slide]

    # transform=ccrs.LambertConformal(central_lon, central_lat), ax=ax2)
    cmap = plt.cm.get_cmap('cividis', 20)
    cmap.set_bad(alpha=0.0)

    waterdepthmap = ax.pcolormesh(lon_rho_a, lat_rho_a, cf, cmap=cmap, norm=norm,
                                  transform=ccrs.PlateCarree(), alpha=0.7)  # ,
    ax_pos = ax.get_position()

    cbar = plt.colorbar(waterdepthmap, ax=ax, pad=0.1)
    cbar.ax.set_position([ax_pos.x1 + 0.01, ax_pos.y0, 0.03, ax_pos.height])
    cbar.ax.set_ylabel('distance in meters between the surface of the water and sea level')

    value_range = np.ptp(u_sub)
    q = ax.quiver(lon_rho_a_sub, lat_rho_a_sub, u_sub, v_sub, pivot='tail', scale=10)
    u_sub = np.nan_to_num(u_sub, nan=-99999)  # Replace NaN with large negative value
    v_sub = np.nan_to_num(v_sub, nan=-99999)
    vector_lengths = np.linalg.norm(np.stack([u, v]), axis=0)
    vector_lengths[vector_lengths < 0] = np.nan
    # vector_lengths_range = np.ptp(vector_lengths)
    vector_lengths_range = (np.nanmin(vector_lengths), np.nanmax(vector_lengths))
    # print(vector_lengths)
    # print(vector_lengths_range)
    veclenght = 0.5
    maxstr = '%3.1f m/s' % veclenght
    font_props = FontProperties(size='12')  # You can change 'large' to a specific number for more control

    plt.quiverkey(q, 0.1, 0.9, veclenght, maxstr, labelpos='S',fontproperties=font_props,
                  coordinates='axes').set_zorder(11)
    # Save the figure
    #test if the file already exists
    formatted_datetime = reformat_datetime(str(ot))

    map_data = {
        'vector_lengths_range': str(vector_lengths_range),
        'date_and_time': str(formatted_datetime),
        'depth_range': str(cfvector_lengths_range),
    }
    with open(data_file, 'w') as file:
        json.dump(map_data, file)
    print("Map data saved:", data_file)
    print('depth range: ' + str(cfvector_lengths_range))
    mapfile = base_path + 'my_map'+ str(time_slide) + '.png'
    fig.savefig(mapfile)  # Saves as PNG file
    plt.close()
    print("done")

    # At the end, save the new map



    print("Map generated and saved:", mapfile)
    return mapfile, data_file # , cfvector_lengths_range

depth_ranges = []
for i in range(1, 500):
    make_map(i)
    # depth_ranges.append(depth_range)

# Printing the depth ranges
# for i, dr in enumerate(depth_ranges):
#     print(f"Map {i+1}: Depth Range: Min = {dr[0]}, Max = {dr[1]}")