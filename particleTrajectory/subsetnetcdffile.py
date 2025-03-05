import xarray as xr
import numpy as np

# Open the NetCDF file
ds = xr.open_dataset('N_1000_GB1_A_20230609_135000_30sec_A_0.100_1_traj.nc', decode_times=False)

# 1. Subset by location/coordinates
# If you have lat/lon coordinates:
# subset = ds.sel(latitude=slice(30, 40),    # Select latitudes between 30-40
#                longitude=slice(-100, -90)) # Select longitudes between -100 and -90

# 2. Subset by time
# If you have a time dimension:
# subset = ds.sel(time=slice('2020-01-01', '2020-12-31'))

# 3. Subset by index
# If you want to use integer positions:
mask = (ds.T >= 739046.52) & (ds.T <= 739046.53)
subset = ds.where(mask, drop=True)
# 4. Subset by condition
# Example: selecting values above a threshold
# subset = ds.where(ds.temperature > 273.15)

# 5. Save the subset to a new NetCDF file
subset.to_netcdf('N_1000_GB1_A_20230609_135000_30sec_A_0_sub.nc')