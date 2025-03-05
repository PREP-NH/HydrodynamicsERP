import numpy as np
import matplotlib.pyplot as plt
import xarray as xr
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import cartopy.io.img_tiles as cimgt
from matplotlib.animation import FuncAnimation
import os
import re
from datetime import datetime
import json


class MultiParticleAnimation:
    def __init__(self, file_list):
        """Initialize with list of NetCDF files."""
        self.file_list = file_list
        self.fig = None
        self.ax = None
        self.scatters = []
        self.data = []
        self.metadata = []

        # Define colors for different files
        self.colors = ['red', 'blue', 'green', 'purple', 'orange', 'cyan', 'magenta',
                       'yellow', 'brown', 'pink', 'gray', 'olive', 'navy', 'teal', 'coral', 'lime']

    def load_all_data(self):
        """Load data from all NetCDF files."""
        for i, file_path in enumerate(self.file_list):
            try:
                ds = xr.open_dataset(file_path, decode_times=False)

                # Extract basic data
                lon = ds.Xll.values
                lat = ds.Yll.values
                time = ds.T.values

                # Get metadata
                filename = os.path.basename(file_path)
                n_particles = int(re.search(r'N_(\d+)', filename).group(1))

                if 'GB1' in filename:
                    gb_match = re.search(r'GB1_([A-Z])', filename)
                    site = f"GB1 {gb_match.group(1)}" if gb_match else "Unknown GB1 Site"
                else:
                    site_match = re.search(r'tier2_site_(\d+)', filename)
                    site = f"Tier 2 Site {site_match.group(1)}" if site_match else "Unknown Site"

                # Store data
                self.data.append({
                    'lon': lon,
                    'lat': lat,
                    'time': time,
                    'color': self.colors[i % len(self.colors)],
                    'site': site,
                    'n_particles': n_particles
                })

                # Store metadata
                self.metadata.append({
                    'filename': filename,
                    'site': site,
                    'n_particles': n_particles,
                    'time_steps': len(time),
                    'lon_range': [float(np.nanmin(lon)), float(np.nanmax(lon))],
                    'lat_range': [float(np.nanmin(lat)), float(np.nanmax(lat))],
                    'total_time_hours': float(time[-1] - time[0]) / 3600,
                    'color': self.colors[i % len(self.colors)]
                })

                print(f"Loaded {filename} - {site} with {n_particles} particles")

            except Exception as e:
                print(f"Error loading {file_path}: {str(e)}")
                continue

        # Save metadata to file
        self.save_metadata()
        return len(self.data) > 0

    def save_metadata(self):
        """Save metadata to JSON file."""
        metadata_output = {
            'animation_info': {
                'total_files': len(self.metadata),
                'creation_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            },
            'files': self.metadata
        }

        with open('particle_animation_metadata.json', 'w') as f:
            json.dump(metadata_output, f, indent=2)

    def setup_map(self):
        """Set up the base map."""
        self.fig = plt.figure(figsize=(12, 10))
        self.ax = plt.axes(projection=ccrs.PlateCarree())

        # Add features
        self.ax.add_feature(cfeature.LAND, facecolor='lightgray', alpha=0.5, zorder=0)
        self.ax.add_feature(cfeature.OCEAN, facecolor='lightblue', alpha=0.5, zorder=1)
        self.ax.add_feature(cfeature.COASTLINE, zorder=2)

        # Add OpenStreetMap tiles
        osm_tiles = cimgt.OSM()
        self.ax.add_image(osm_tiles, 12, zorder=3)

        # Add gridlines
        gl = self.ax.gridlines(draw_labels=True, zorder=4)
        gl.top_labels = False
        gl.right_labels = False

        # Initialize scatter plots
        self.scatters = []
        for d in self.data:
            scatter = self.ax.scatter([], [],
                                      c=d['color'],
                                      alpha=0.6,
                                      s=20,
                                      transform=ccrs.PlateCarree(),
                                      label=d['site'],
                                      zorder=5)
            self.scatters.append(scatter)

        # Calculate overall extent
        all_lons = np.concatenate([d['lon'].flatten() for d in self.data])
        all_lats = np.concatenate([d['lat'].flatten() for d in self.data])

        lon_buffer = 0.02
        lat_buffer = 0.02
        lon_min = np.nanmin(all_lons)
        lon_max = np.nanmax(all_lons)
        lat_min = np.nanmin(all_lats)
        lat_max = np.nanmax(all_lats)

        self.ax.set_extent([
            lon_min - lon_buffer,
            lon_max + lon_buffer,
            lat_min - lat_buffer,
            lat_max + lat_buffer
        ])

        # Add legend
        self.ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.tight_layout()

        return self.fig, self.ax

    def update(self, frame):
        """Update function for animation."""
        for i, (scatter, data) in enumerate(zip(self.scatters, self.data)):
            valid_mask = ~np.isnan(data['lon'][frame]) & ~np.isnan(data['lat'][frame])
            valid_coords = np.c_[data['lon'][frame][valid_mask], data['lat'][frame][valid_mask]]

            if len(valid_coords) > 0:
                scatter.set_offsets(valid_coords)

        # Use time from first dataset for title
        time_hours = self.data[0]['time'][frame] / 3600
        self.ax.set_title(f'Particle Positions - Time: {time_hours:.1f} hours')

        return self.scatters

    def create_animation(self, output_file='particle_animation.mp4', fps=10):
        """Create and save the animation."""
        if not self.load_all_data():
            return

        # Set up the map
        self.setup_map()

        # Get minimum number of frames across all datasets
        min_frames = min(d['lon'].shape[0] for d in self.data)
        frames_to_use = min(min_frames, 1000)  # Limit frames to prevent memory issues

        print(f"Saving animation with {frames_to_use} frames to {output_file}...")

        anim = FuncAnimation(
            self.fig,
            self.update,
            frames=frames_to_use,
            interval=1000 / fps,
            blit=True
        )

        # Save animation as MP4
        anim.save(output_file, writer='ffmpeg', fps=fps)
        plt.close()

        print(f"Animation saved to {output_file}")


def main():
    # List of files to process
    files = [
        "N_1000_GB1_A_20230609_135000_30sec_A_0.100_1_traj.nc",
        "N_1000_GB1_A_20230609_135000_30sec_A_0.100_2_traj.nc",
        "N_1000_GB1_A_20230609_135000_30sec_A_0.100_3_traj.nc",
        "N_1000_GB1_A_20230609_135000_30sec_A_0.100_4_traj.nc",
        "N_1000_GB1_A_20230609_135000_30sec_A_0.100_5_traj.nc",
        "N_1000_GB1_A_20230609_135000_30sec_A_0.100_6_traj.nc",
        "N_1000_GB1_A_20230609_135000_30sec_A_0.100_7_traj.nc",
        "N_1000_GB1_A_20230609_135000_30sec_A_0.100_8_traj.nc",
        "N_1000_tier2_site_85_30sec_A_0.100_plus6.25hrs_1_traj.nc",
        "N_1000_tier2_site_85_30sec_A_0.100_plus6.25hrs_2_traj.nc",
        "N_1000_tier2_site_85_30sec_A_0.100_plus6.25hrs_3_traj.nc",
        "N_1000_tier2_site_85_30sec_A_0.100_plus6.25hrs_4_traj.nc",
        "N_1000_tier2_site_85_30sec_A_0.100_plus6.25hrs_6_traj.nc",
        "N_1000_tier2_site_85_30sec_A_0.100_plus6.25hrs_7_traj.nc",
        "N_1000_tier2_site_85_30sec_A_0.100_plus6.25hrs_8_traj.nc"
    ]

    animator = MultiParticleAnimation(files)
    animator.create_animation(output_file='particle_trajectories.mp4', fps=10)


if __name__ == "__main__":
    main()