import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import contextily as ctx
import matplotlib.colors as colors
from matplotlib.font_manager import FontProperties


def make_map(time_slide):
    # Open the subsetted NetCDF file
    ds = xr.open_dataset('N_1000_GB1_A_20230609_135000_30sec_A_0.100_2_traj.nc', decode_times=False)

    # Extract coordinates
    lon = ds.Xcart.values  # X coordinate
    lat = ds.Ycart.values  # Y coordinate

    print("Coordinate shapes:")
    print(f"Longitude shape: {lon.shape}")
    print(f"Latitude shape: {lat.shape}")

    # Remove NaN and infinite values
    valid_mask = np.isfinite(lon) & np.isfinite(lat)
    valid_lon = lon[valid_mask]
    valid_lat = lat[valid_mask]

    print("Valid coordinate ranges:")
    print(f"Longitude range: {valid_lon.min()} to {valid_lon.max()}")
    print(f"Latitude range: {valid_lat.min()} to {valid_lat.max()}")

    if len(valid_lon) == 0 or len(valid_lat) == 0:
        print("No valid coordinate data found")
        return None

    # Get map boundaries from valid data in meters
    min_lon = np.min(valid_lon)
    max_lon = np.max(valid_lon)
    min_lat = np.min(valid_lat)
    max_lat = np.max(valid_lat)

    # Add a small buffer to the extent
    lon_buffer = (max_lon - min_lon) * 0.1
    lat_buffer = (max_lat - min_lat) * 0.1

    min_lon -= lon_buffer
    max_lon += lon_buffer
    min_lat -= lat_buffer
    max_lat += lat_buffer

    # Create figure
    fig = plt.figure(figsize=(11, 5))
    ax = plt.axes()

    # Plot points with a simple color
    scatter = ax.scatter(valid_lon, valid_lat,
                         c='blue',
                         alpha=0.5,
                         s=1)  # small point size

    # Set axis labels
    ax.set_xlabel('Distance East (m)')
    ax.set_ylabel('Distance North (m)')

    # Add title
    plt.title(f'Particle Positions at Time Step: {time_slide}')

    # Make the plot look nice
    ax.grid(True, linestyle='--', alpha=0.6)
    ax.set_aspect('equal')  # maintain aspect ratio

    # Save figure
    mapfile = f'./maps/subset_map_2_{time_slide}.png'
    plt.savefig(mapfile, dpi=300, bbox_inches='tight')
    plt.close()

    return mapfile


# Generate maps for a range of timesteps
n_timesteps = 20  # Adjust based on your needs
for i in range(n_timesteps):
    print(f"Processing time step {i}")
    make_map(i)