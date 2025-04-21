"""
Create videos for sites using the optimized MultiParticleAnimation class.
Parallelized implementation for improved performance.
"""

import os
import json
import argparse
import re
from datetime import datetime
import numpy as np
import cupy as cp
import subprocess
import matplotlib
import concurrent.futures  # For parallel execution
from concurrent.futures import ThreadPoolExecutor, as_completed

matplotlib.use('Agg')  # Use non-interactive backend for parallel processing
import matplotlib.pyplot as plt
import xarray as xr
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import cartopy.io.img_tiles as cimgt
from matplotlib.animation import FuncAnimation
import gc  # For garbage collection
import multiprocessing
from functools import partial
import time
import logging

# Set up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler()])
logger = logging.getLogger('particle_animation')


class MultiParticleAnimation:
    def __init__(self, file_list, particle_subsample=1, time_stride=1):
        """
        Initialize with list of NetCDF files.

        Args:
            file_list: List of trajectory files to process
            particle_subsample: Only use every Nth particle (default: 1, use all)
            time_stride: Only use every Nth time step (default: 1, use all)
        """
        self.file_list = file_list
        self.particle_subsample = particle_subsample
        self.time_stride = time_stride
        self.fig = None
        self.ax = None
        self.scatters = []
        self.data = []
        self.metadata = []
        self.custom_extent = None
        self.map_extent = None
        self.use_gpu = True

        # Define colors for different files
        self.colors = ['red', 'blue', 'green', 'purple', 'orange', 'cyan', 'magenta',
                       'yellow', 'brown', 'pink', 'gray', 'olive', 'navy', 'teal', 'coral', 'lime']

    def get_data_extent(self):
        """Calculate the geographic extent of the data without loading everything."""
        all_lon_mins, all_lon_maxs = [], []
        all_lat_mins, all_lat_maxs = [], []

        for file_path in self.file_list:
            try:
                # Use xarray to efficiently get min/max values without loading all data
                with xr.open_dataset(file_path, decode_times=False) as ds:
                    # Get min/max directly from dataset attributes if available
                    # Otherwise compute them from a sample of the data
                    lon = ds.Xll.values
                    lat = ds.Yll.values

                    # Take a small sample to calculate bounds efficiently
                    # Sample every 100th time step and every 10th particle
                    lon_sample = lon[::100, ::10]
                    lat_sample = lat[::100, ::10]
                    # First, convert NumPy arrays to CuPy arrays
                    lon_sample_gpu = cp.asarray(lon_sample)
                    lat_sample_gpu = cp.asarray(lat_sample)

                    # Filter out NaN values
                    valid_mask_lon = ~cp.isnan(lon_sample_gpu)
                    valid_mask_lat = ~cp.isnan(lat_sample_gpu)

                    valid_lons = lon_sample_gpu[valid_mask_lon]
                    valid_lats = lat_sample_gpu[valid_mask_lat]

                    if valid_lons.size > 0:  # Use .size instead of len()
                        all_lon_mins.append(float(cp.min(valid_lons).get()))  # Convert back to Python scalar
                        all_lon_maxs.append(float(cp.max(valid_lons).get()))  # Convert back to Python scalar

                    if valid_lats.size > 0:  # Use .size instead of len()
                        all_lat_mins.append(float(cp.min(valid_lats).get()))  # Convert back to Python scalar
                        all_lat_maxs.append(float(cp.max(valid_lats).get()))  # Convert back to Python scalar

            except Exception as e:
                logger.error(f"Error reading bounds from {file_path}: {str(e)}")
                continue

        if not all_lon_mins or not all_lat_mins:
            # Default to Great Bay area if no valid coordinates
            return [-70.9, -70.7, 43.0, 43.2]

        # Calculate overall extent
        lon_min = min(all_lon_mins)
        lon_max = max(all_lon_maxs)
        lat_min = min(all_lat_mins)
        lat_max = max(all_lat_maxs)

        # Add buffer (approximately 1 km)
        lat_center = (lat_min + lat_max) / 2
        lon_buffer = 1 / (111 *cp.cos(cp.radians(lat_center)))
        lat_buffer = 1 / 111

        return [
            lon_min - lon_buffer,
            lon_max + lon_buffer,
            lat_min - lat_buffer,
            lat_max + lat_buffer
        ]

    def create_animation(self, output_file='particle_animation.mp4', fps=10, max_frames=None, reverse=False):
        """Create and save the animation."""
        # Check if GPU acceleration is enabled
        if hasattr(self, 'use_gpu') and self.use_gpu:
            logger.info(f"Using GPU-accelerated animation creation")
            return self.create_parallel_animation(output_file, fps, max_frames, reverse)
        else:
            # Original CPU implementation
            return self._create_animation_cpu(output_file, fps, max_frames, reverse)

    def get_file_info(self):
        """Get basic info about the files without loading all data."""
        file_info = []

        for i, file_path in enumerate(self.file_list):
            try:
                with xr.open_dataset(file_path, decode_times=False) as ds:
                    # Get dimensions without loading all data
                    time_shape = ds.T.shape[0]

                    # Check if time is decreasing instead of increasing
                    time_decreasing = (ds.T.values[0] > ds.T.values[-1]) if time_shape > 1 else False

                    # Get metadata
                    filename = os.path.basename(file_path)
                    n_particles = int(re.search(r'N_(\d+)', filename).group(1))

                    # Extract site information
                    if 'GB1' in filename:
                        gb_match = re.search(r'GB1_([A-Z])', filename)
                        site = f"GB1_{gb_match.group(1)}" if gb_match else "Unknown_GB1_Site"
                    elif 'GB2' in filename:
                        gb_match = re.search(r'GB2_([A-Z])', filename)
                        site = f"GB2_{gb_match.group(1)}" if gb_match else "Unknown_GB2_Site"
                    elif 'GB3' in filename:
                        gb_match = re.search(r'GB3_([A-Z])', filename)
                        site = f"GB3_{gb_match.group(1)}" if gb_match else "Unknown_GB3_Site"
                    else:
                        site_match = re.search(r'tier2_site_(\d+)', filename)
                        site = f"Tier2_Site_{site_match.group(1)}" if site_match else "Unknown_Site"

                    # Extract date information if available
                    date_match = re.search(r'_(\d{8})_', filename)
                    date = date_match.group(1) if date_match else None

                    file_info.append({
                        'filename': filename,
                        'path': file_path,
                        'site': site,
                        'date': date,
                        'n_particles': n_particles,
                        'time_steps': time_shape,
                        'time_decreasing': time_decreasing,
                        'color': self.colors[i % len(self.colors)]
                    })

                    logger.info(
                        f"Processed info for {filename} - {site} with {n_particles} particles (time {'decreasing' if time_decreasing else 'increasing'})")

            except Exception as e:
                logger.error(f"Error getting info from {file_path}: {str(e)}")
                continue

        return file_info

    def load_frame_data(self, file_info, frame_index):
        """Load data for a specific frame from all files."""
        frame_data = []

        for info in file_info:
            try:
                with xr.open_dataset(info['path'], decode_times=False) as ds:
                    # Load only the specific frame we need
                    if frame_index >= ds.T.shape[0] or frame_index < 0:
                        # Skip if frame is out of bounds
                        frame_data.append({
                            'lon': np.array([]),
                            'lat': np.array([]),
                            'color': info['color'],
                            'site': info['site']
                        })
                        continue

                    # No need to check time_decreasing here - frame_index already accounts for correct direction

                    # Load only the specific frame we need
                    lon = ds.Xll.values[frame_index]
                    lat = ds.Yll.values[frame_index]

                    # Subsample particles if needed
                    if self.particle_subsample > 1:
                        lon = lon[::self.particle_subsample]
                        lat = lat[::self.particle_subsample]

                    frame_data.append({
                        'lon': lon,
                        'lat': lat,
                        'color': info['color'],
                        'site': info['site']
                    })

            except Exception as e:
                logger.error(f"Error loading frame {frame_index} from {info['path']}: {str(e)}")
                frame_data.append({
                    'lon': np.array([]),
                    'lat': np.array([]),
                    'color': info['color'],
                    'site': info['site']
                })

        return frame_data

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

        # Get file info for setup
        file_info = self.get_file_info()

        # Initialize scatter plots
        self.scatters = []
        for info in file_info:
            scatter = self.ax.scatter([], [],
                                      c=info['color'],
                                      alpha=0.6,
                                      s=20,
                                      transform=ccrs.PlateCarree(),
                                      label=info['site'],
                                      zorder=5)
            self.scatters.append(scatter)

        # Set map extent
        if self.custom_extent:
            logger.info(f"Using custom map extent: {self.custom_extent}")
            self.ax.set_extent(self.custom_extent)
            self.map_extent = self.custom_extent
        else:
            # Calculate extent from all files
            self.map_extent = self.get_data_extent()
            self.ax.set_extent(self.map_extent)
            logger.info(f"Calculated map extent: {self.map_extent}")

        # Add legend
        self.ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.tight_layout()

        return self.fig, self.ax, file_info

    def update(self, frame_index):
        """Update function for animation."""
        # Get info about files
        file_info = getattr(self, '_file_info', self.get_file_info())

        # Load data for this specific frame
        frame_data = self.load_frame_data(file_info, frame_index)

        # Update scatter plots
        for i, (scatter, data) in enumerate(zip(self.scatters, frame_data)):
            valid_mask = ~cp.isnan(data['lon']) & ~cp.isnan(data['lat'])
            valid_coords = cp.c_[data['lon'][valid_mask], data['lat'][valid_mask]]

            if len(valid_coords) > 0:
                scatter.set_offsets(valid_coords)
            else:
                scatter.set_offsets(cp.empty((0, 2)))

        # Use time from the first file for title if possible
        try:
            with xr.open_dataset(file_info[0]['path'], decode_times=False) as ds:
                time_vals = ds.T.values
                # Check if time is decreasing
                time_decreasing = time_vals[0] > time_vals[-1]
                actual_index = ds.T.shape[0] - 1 - frame_index if time_decreasing else frame_index

                if actual_index < len(time_vals):
                    time_hours = time_vals[actual_index]

                    # Convert to hours since start if needed
                    if time_hours > 100000:  # Probably Excel/MATLAB date format
                        time_ref = time_vals[0] if not time_decreasing else time_vals[-1]
                        time_hours = (time_hours - time_ref) * 24  # Convert days to hours

                    time_label = f"{abs(time_hours):.2f} hours"
                    self.ax.set_title(f'Particle Positions - Time: {time_label}')
                else:
                    self.ax.set_title(f'Particle Positions - Frame {frame_index}')
        except Exception as e:
            # Fallback title if we can't get time
            self.ax.set_title(f'Particle Positions - Frame {frame_index}')

        # Force garbage collection to minimize memory usage
        gc.collect()

        return self.scatters

    def _create_animation_cpu(self, output_file='particle_animation.mp4', fps=10, max_frames=None, reverse=False):
        """Create and save the animation with CPU processing (original implementation)."""
        # Original implementation goes here
        try:
            # Set up the map and get file info
            self.fig, self.ax, file_info = self.setup_map()
            self._file_info = file_info

            # Original code...
            # ...
        except Exception as e:
            logger.error(f"Error creating animation: {str(e)}")
            plt.close()
            return False, None

    def render_frame_gpu(frame_index, file_info, particle_subsample, map_extent):
        """Render a single frame using GPU acceleration."""
        frame_data = []

        for info in file_info:
            try:
                with xr.open_dataset(info['path'], decode_times=False) as ds:
                    if frame_index >= ds.T.shape[0] or frame_index < 0:
                        # Skip if frame is out of bounds
                        frame_data.append({
                            'lon': np.array([]),
                            'lat': np.array([]),
                            'color': info['color'],
                            'site': info['site']
                        })
                        continue

                    # Load data to GPU memory
                    lon_cpu = ds.Xll.values[frame_index]
                    lat_cpu = ds.Yll.values[frame_index]

                    # Transfer to GPU
                    lon = cp.array(lon_cpu)
                    lat = cp.array(lat_cpu)

                    # Subsample particles
                    if particle_subsample > 1:
                        lon = lon[::particle_subsample]
                        lat = lat[::particle_subsample]

                    # Filter valid points on GPU
                    valid_mask = ~cp.isnan(lon) & ~cp.isnan(lat) & \
                                 (lon >= map_extent[0]) & (lon <= map_extent[1]) & \
                                 (lat >= map_extent[2]) & (lat <= map_extent[3])

                    # Get valid coordinates
                    valid_lon = lon[valid_mask]
                    valid_lat = lat[valid_mask]

                    # Transfer results back to CPU when needed
                    frame_data.append({
                        'lon': cp.asnumpy(valid_lon) if valid_lon.size > 0 else np.array([]),
                        'lat': cp.asnumpy(valid_lat) if valid_lat.size > 0 else np.array([]),
                        'color': info['color'],
                        'site': info['site']
                    })

            except Exception as e:
                logging.error(f"Error rendering frame {frame_index} from {info['path']}: {str(e)}")
                frame_data.append({
                    'lon': np.array([]),
                    'lat': np.array([]),
                    'color': info['color'],
                    'site': info['site']
                })

        return frame_index, frame_data

    def create_parallel_animation(self, output_file='particle_animation.mp4', fps=10, max_frames=None, reverse=False):
        """Create animation with parallel GPU-accelerated frame rendering."""
        try:
            # Set up the map and get file info
            self.fig, self.ax, file_info = self.setup_map()
            self._file_info = file_info  # Store for use in update function

            # Get minimum number of frames across all datasets
            min_frames = min(info['time_steps'] for info in file_info)

            # Apply stride
            total_stride = self.calculate_stride(min_frames, max_frames)
            frame_indices = self.get_frame_indices(min_frames, total_stride, max_frames)

            logger.info(
                f"Creating parallel animation with {len(frame_indices)} frames using time stride {total_stride}...")

            # Create frame data for each index
            all_frames = []
            temp_dir = os.path.join(os.path.dirname(output_file), "temp_frames")
            os.makedirs(temp_dir, exist_ok=True)

            try:
                # Use GPU acceleration if available
                if self.use_gpu and self.gpu_provider == 'cupy':
                    import cupy as cp
                    for i, frame_idx in enumerate(frame_indices):
                        try:
                            # Load frame data using GPU
                            gpu_frame_data = []
                            for info in file_info:
                                with xr.open_dataset(info['path'], decode_times=False) as ds:
                                    if frame_idx >= ds.T.shape[0] or frame_idx < 0:
                                        # Skip if frame is out of bounds
                                        gpu_frame_data.append({
                                            'lon': np.array([]),
                                            'lat': np.array([]),
                                            'color': info['color'],
                                            'site': info['site']
                                        })
                                        continue

                                    # Load data
                                    lon_cpu = ds.Xll.values[frame_idx]
                                    lat_cpu = ds.Yll.values[frame_idx]

                                    # Transfer to GPU
                                    lon = cp.array(lon_cpu)
                                    lat = cp.array(lat_cpu)

                                    # Subsample particles
                                    if self.particle_subsample > 1:
                                        lon = lon[::self.particle_subsample]
                                        lat = lat[::self.particle_subsample]

                                    # Filter valid points on GPU
                                    valid_mask = ~cp.isnan(lon) & ~cp.isnan(lat)

                                    # Get valid coordinates
                                    valid_lon = lon[valid_mask]
                                    valid_lat = lat[valid_mask]

                                    # Transfer results back to CPU
                                    gpu_frame_data.append({
                                        'lon': cp.asnumpy(valid_lon) if valid_lon.size > 0 else np.array([]),
                                        'lat': cp.asnumpy(valid_lat) if valid_lat.size > 0 else np.array([]),
                                        'color': info['color'],
                                        'site': info['site']
                                    })

                            # Update plot with frame data
                            for j, (scatter, data) in enumerate(zip(self.scatters, gpu_frame_data)):
                                if len(data['lon']) > 0:
                                    scatter.set_offsets(np.c_[data['lon'], data['lat']])
                                else:
                                    scatter.set_offsets(np.empty((0, 2)))

                            # Update title
                            self.update_title(frame_idx)

                            # Save frame
                            frame_file = os.path.join(temp_dir, f"frame_{i:06d}.png")
                            self.fig.savefig(frame_file, dpi=100)
                            all_frames.append(frame_file)

                            if i % 10 == 0:  # Progress logging
                                logger.info(f"Rendered {i}/{len(frame_indices)} frames")

                        except Exception as e:
                            logger.error(f"Error processing frame {frame_idx}: {str(e)}")

                    # Force cleanup
                    cp.get_default_memory_pool().free_all_blocks()

                else:
                    # CPU fallback
                    logger.info("Using CPU for frame rendering (no GPU acceleration)")
                    for i, frame_idx in enumerate(frame_indices):
                        # Load frame data using standard CPU approach
                        frame_data = self.load_frame_data(file_info, frame_idx)

                        # Update plot with frame data
                        for j, (scatter, data) in enumerate(zip(self.scatters, frame_data)):
                            if len(data['lon']) > 0:
                                scatter.set_offsets(np.c_[data['lon'], data['lat']])
                            else:
                                scatter.set_offsets(np.empty((0, 2)))

                        # Update title
                        self.update_title(frame_idx)

                        # Save frame
                        frame_file = os.path.join(temp_dir, f"frame_{i:06d}.png")
                        self.fig.savefig(frame_file, dpi=100)
                        all_frames.append(frame_file)

                        if i % 10 == 0:  # Progress logging
                            logger.info(f"Rendered {i}/{len(frame_indices)} frames")

                # Use FFmpeg to combine frames into video
                self.encode_video(all_frames, output_file, fps)

                # Cleanup
                plt.close(self.fig)
                for file in all_frames:
                    try:
                        os.remove(file)
                    except:
                        pass
                try:
                    os.rmdir(temp_dir)
                except:
                    pass

                return True, file_info

            except Exception as e:
                logger.error(f"Error in parallel animation creation: {str(e)}")
                if hasattr(self, 'fig') and self.fig is not None:
                    plt.close(self.fig)

                # Clean up any temp files
                for file in all_frames:
                    try:
                        os.remove(file)
                    except:
                        pass
                try:
                    os.rmdir(temp_dir)
                except:
                    pass

                return False, None

        except Exception as e:
            logger.error(f"Error in parallel animation creation: {str(e)}")
            if hasattr(self, 'fig') and self.fig is not None:
                plt.close(self.fig)
            return False, None

    def encode_video(self, frame_files, output_file, fps):
        """Use FFmpeg to create video from frames."""
        try:
            # Create output directory if it doesn't exist
            os.makedirs(os.path.dirname(output_file), exist_ok=True)

            # Check if we have GPU encoding available
            if hasattr(self, 'use_gpu') and self.use_gpu:
                # Try hardware acceleration with GPU
                try:
                    # NVIDIA GPU encoding
                    cmd = [
                        'ffmpeg', '-y',
                        '-framerate', str(fps),
                        '-pattern_type', 'glob',
                        '-i', os.path.join(os.path.dirname(frame_files[0]), 'frame_*.png'),
                        '-c:v', 'h264_nvenc',
                        '-preset', 'fast',
                        '-pix_fmt', 'yuv420p',
                        output_file
                    ]

                    logger.info(f"Trying GPU-accelerated video encoding")
                    subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    logger.info(f"Successfully created video using GPU acceleration: {output_file}")
                    return True
                except:
                    logger.warning("GPU video encoding failed, falling back to CPU")

            # Standard FFmpeg encoding (CPU)
            cmd = [
                'ffmpeg', '-y',
                '-framerate', str(fps),
                '-pattern_type', 'glob',
                '-i', os.path.join(os.path.dirname(frame_files[0]), 'frame_*.png'),
                '-c:v', 'libx264',
                '-preset', 'fast',
                '-pix_fmt', 'yuv420p',
                output_file
            ]

            subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            logger.info(f"Successfully created video: {output_file}")
            return True

        except Exception as e:
            logger.error(f"Error in video encoding: {str(e)}")

            # Fallback to matplotlib writer
            try:
                logger.info("Trying fallback encoding with matplotlib...")
                Writer = matplotlib.animation.writers['ffmpeg']
                writer = Writer(fps=fps, metadata=dict(artist='Particle Trajectory Script'), bitrate=1800)

                # Create a new figure with the last frame data
                fig, ax = plt.subplots(figsize=(12, 10))
                ax.set_xlim(self.map_extent[0], self.map_extent[1])
                ax.set_ylim(self.map_extent[2], self.map_extent[3])

                # Add the frames manually
                with writer.saving(fig, output_file, dpi=100):
                    for frame_file in frame_files:
                        img = plt.imread(frame_file)
                        ax.imshow(img, extent=self.map_extent)
                        writer.grab_frame()
                        ax.clear()

                plt.close(fig)
                logger.info("Fallback encoding successful")
                return True

            except Exception as fallback_error:
                logger.error(f"Fallback encoding also failed: {str(fallback_error)}")
                return False

    def _create_animation_cpu(self, output_file='particle_animation.mp4', fps=10, max_frames=None, reverse=False):
        """Create and save the animation with CPU processing (original implementation)."""
        # Original implementation goes here
        try:
            # Set up the map and get file info
            self.fig, self.ax, file_info = self.setup_map()
            self._file_info = file_info

            # Original code...
            # ...
        except Exception as e:
            logger.error(f"Error creating animation: {str(e)}")
            plt.close()
            return False, None
    def process_in_batches(self, frame_indices, batch_size=10):
        """Process frames in batches to manage memory usage."""
        all_results = []

        for i in range(0, len(frame_indices), batch_size):
            batch = frame_indices[i:i + batch_size]
            logger.info(f"Processing batch {i // batch_size + 1}/{(len(frame_indices) + batch_size - 1) // batch_size}")

            with ThreadPoolExecutor(max_workers=len(batch)) as executor:
                futures = {
                    executor.submit(
                        render_frame_gpu,
                        frame_idx,
                        self._file_info,
                        self.particle_subsample,
                        self.map_extent
                    ): frame_idx
                    for frame_idx in batch
                }

                for future in as_completed(futures):
                    result = future.result()
                    all_results.append(result)

            # Force cleanup between batches
            cp.get_default_memory_pool().free_all_blocks()

        return sorted(all_results, key=lambda x: x[0])

    def optimize_memory_usage(self):
        """Optimize GPU memory usage based on dataset size."""
        # Estimate memory requirements
        total_particles = sum(info['n_particles'] for info in self._file_info)
        estimated_bytes_per_frame = total_particles * 2 * 8  # Two float64 arrays (lon/lat)

        # Get available GPU memory
        try:
            import pycuda.driver as cuda
            free_mem, total_mem = cuda.mem_get_info()
            free_mem_mb = free_mem / (1024 * 1024)

            logger.info(f"Available GPU memory: {free_mem_mb:.2f} MB")
            logger.info(f"Estimated memory per frame: {estimated_bytes_per_frame / (1024 * 1024):.2f} MB")

            # Adjust batch size based on available memory (use 70% of free memory)
            max_frames_in_memory = int(0.7 * free_mem / estimated_bytes_per_frame)
            return max(1, max_frames_in_memory)

        except:
            # Fallback to conservative estimate if can't query GPU
            logger.warning("Could not query GPU memory, using conservative batch size")
            return 10  # Process 10 frames at a time as fallback

    def calculate_stride(self, min_frames, max_frames):
        """Calculate appropriate stride to achieve desired frame count."""
        effective_frames = min_frames // self.time_stride

        if max_frames is not None and effective_frames > max_frames:
            # Calculate a new stride that will give us the desired number of frames
            additional_stride = (effective_frames + max_frames - 1) // max_frames
            total_stride = self.time_stride * additional_stride
            logger.info(f"Adjusting stride from {self.time_stride} to {total_stride} to limit frames to {max_frames}")
            return total_stride
        else:
            # Use the original stride
            return self.time_stride

    def get_frame_indices(self, min_frames, total_stride, max_frames=None):
        """Create the list of frame indices to process with stride."""
        effective_frames = min_frames // total_stride
        frames_to_use = min(effective_frames, max_frames) if max_frames else effective_frames

        # Look through the file info to determine if we should reverse
        should_reverse = any(info.get('time_decreasing', False) for info in self._file_info)

        if should_reverse:
            # Play from end to beginning if time is decreasing
            logger.info("Time is decreasing in data; creating frames in reverse order")
            return list(range(min_frames - 1, -1, -total_stride))[:frames_to_use]
        else:
            # Play from beginning to end (normal)
            return list(range(0, min_frames, total_stride))[:frames_to_use]

    def update_title(self, frame_idx):
        """Update the plot title with the current time information."""
        try:
            if not hasattr(self, '_file_info') or not self._file_info:
                self.ax.set_title(f'Particle Positions - Frame {frame_idx}')
                return

            with xr.open_dataset(self._file_info[0]['path'], decode_times=False) as ds:
                time_vals = ds.T.values
                time_decreasing = time_vals[0] > time_vals[-1]
                actual_index = ds.T.shape[0] - 1 - frame_idx if time_decreasing else frame_idx

                if actual_index < len(time_vals):
                    time_hours = time_vals[actual_index]

                    # Convert to hours since start if needed
                    if time_hours > 100000:  # Probably Excel/MATLAB date format
                        time_ref = time_vals[0] if not time_decreasing else time_vals[-1]
                        time_hours = (time_hours - time_ref) * 24  # Convert days to hours

                    time_label = f"{abs(time_hours):.2f} hours"
                    self.ax.set_title(f'Particle Positions - Time: {time_label}')
                else:
                    self.ax.set_title(f'Particle Positions - Frame {frame_idx}')
        except Exception as e:
            # Fallback title if we can't get time
            self.ax.set_title(f'Particle Positions - Frame {frame_idx}')


def process_single_config(config_item, input_dir, output_dir, global_extent,
                          particle_subsample, time_stride, max_frames, all_tstride_only,
                          fps=10, reverse=False, use_gpu=False, gpu_config=None,
                          gpu_batch_size=None, max_workers=None):
    """
    Process a single site/date configuration to create video(s).
    This function will be called by each worker process.

    Args:
        config_item: Tuple of (config_key, config_dict)
        input_dir: Directory containing trajectory files
        output_dir: Directory to save videos
        global_extent: Map extent to use for consistent dimensions
        particle_subsample: Only use every Nth particle
        time_stride: Only use every Nth time step
        max_frames: Maximum number of frames per video
        all_tstride_only: If True, only use all_tstride files
        fps: Frames per second in output video
        reverse: Whether to reverse animation playback
        use_gpu: Whether to use GPU acceleration
        gpu_config: Dictionary with GPU configuration
        gpu_batch_size: Number of frames to process in parallel on GPU
        max_workers: Maximum number of worker threads for frame rendering

    Returns:
        dict: Results of processing the configuration
    """
    config_key, config = config_item
    process_id = os.getpid()

    try:
        # Get site and date information
        site = config['site']
        date = config['date']

        logger.info(f"Process {process_id}: Starting {site} on {date}")

        # If all_tstride_only is enabled, we'll only have all_tstride files in the config
        if all_tstride_only:
            all_tstride_files = config['trajectory_files']  # All files are all_tstride
            regular_files = []  # No regular files
        else:
            # Separate regular files from all_tstride files
            regular_files = [f for f in config['trajectory_files'] if "all_tstride" not in f]
            all_tstride_files = [f for f in config['trajectory_files'] if "all_tstride" in f]

        results = {
            'site': site,
            'date': date,
            'regular_success': False,
            'all_tstride_success': False,
            'errors': [],
            'regular_metadata': None,
            'all_tstride_metadata': None,
            'gpu_used': use_gpu
        }

        # Group 1: Regular files (skip if all_tstride_only is True)
        if regular_files and not all_tstride_only:
            logger.info(
                f"Process {process_id}: Creating regular video for {site} on {date} with {len(regular_files)} trajectory files")

            # Get full paths to trajectory files
            full_paths = [os.path.join(input_dir, filename) for filename in regular_files]

            # Check if files exist
            existing_paths = [path for path in full_paths if os.path.exists(path)]
            missing_files = [path for path in full_paths if not os.path.exists(path)]

            if missing_files:
                warning_msg = f"Process {process_id}: Warning: {len(missing_files)} files do not exist. First missing: {missing_files[0]}"
                logger.warning(warning_msg)
                results['errors'].append(warning_msg)

            if not existing_paths:
                error_msg = f"Process {process_id}: Error: No valid files found for {site} on {date}"
                logger.error(error_msg)
                results['errors'].append(error_msg)
            else:
                try:
                    # Create animation
                    animator = MultiParticleAnimation(existing_paths,
                                                     particle_subsample=particle_subsample,
                                                     time_stride=time_stride)

                    # Set GPU options
                    if use_gpu and gpu_config and gpu_config['provider'] is not None:
                        animator.use_gpu = True
                        animator.gpu_provider = gpu_config['provider']
                        animator.gpu_batch_size = gpu_batch_size if gpu_batch_size else gpu_config['batch_size']
                        animator.max_workers = max_workers if max_workers else gpu_config['max_workers']
                        logger.info(f"Process {process_id}: Using GPU acceleration with {animator.gpu_provider}")
                    else:
                        animator.use_gpu = False
                        logger.info(f"Process {process_id}: Using CPU processing")

                    # If we have a global extent, use it for all videos
                    if global_extent:
                        animator.custom_extent = global_extent

                    # Get file info to extract site names for filename
                    file_info = animator.get_file_info()

                    # Extract unique site names and dates
                    site_names = sorted(list(set(info['site'] for info in file_info if info['site'])))
                    dates = sorted(list(set(info['date'] for info in file_info if info['date'])))

                    # Create concatenated site name for filename
                    concatenated_site_names = "_".join(site_names)

                    # Use date from filename if available, otherwise use config date
                    date_str = dates[0] if dates else date

                    # Create output filename with concatenated site names
                    output_file = os.path.join(output_dir, f"{concatenated_site_names}_{date_str}.mp4")

                    # Create parent directory if it doesn't exist
                    os.makedirs(os.path.dirname(output_file), exist_ok=True)

                    # Skip if the file already exists
                    if os.path.exists(output_file):
                        logger.info(f"Process {process_id}: Video already exists: {output_file}")
                        results['regular_success'] = True
                    else:
                        success, file_info = animator.create_animation(output_file=output_file, fps=fps,
                                                                     max_frames=max_frames,
                                                                     reverse=reverse)

                        if success:
                            logger.info(f"Process {process_id}: Successfully created video: {output_file}")
                            results['regular_success'] = True

                            # Create metadata file
                            metadata_file = output_file.replace('.mp4', '_metadata.json')
                            metadata = {
                                'video_file': os.path.basename(output_file),
                                'creation_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                'gpu_used': animator.use_gpu if hasattr(animator, 'use_gpu') else False,
                                'gpu_provider': animator.gpu_provider if hasattr(animator, 'gpu_provider') else None,
                                'files_used': [
                                    {
                                        'filename': os.path.basename(info['path']),
                                        'site': info['site'],
                                        'date': info['date'] if info['date'] else '',
                                        'n_particles': info['n_particles'],
                                        'time_steps': info['time_steps']
                                    } for info in file_info
                                ]
                            }

                            with open(metadata_file, 'w') as f:
                                json.dump(metadata, f, indent=2)

                            logger.info(f"Process {process_id}: Created metadata file: {metadata_file}")
                            results['regular_metadata'] = metadata
                        else:
                            error_msg = f"Process {process_id}: Failed to create video for {site} on {date}"
                            logger.error(error_msg)
                            results['errors'].append(error_msg)
                except Exception as e:
                    error_msg = f"Process {process_id}: Error creating video for {site} on {date}: {str(e)}"
                    logger.error(error_msg)
                    results['errors'].append(error_msg)

                # Clean up memory
                del animator
                gc.collect()

        # Group 2: all_tstride files
        if all_tstride_files:
            logger.info(
                f"Process {process_id}: Creating all_tstride video for {site} on {date} with {len(all_tstride_files)} trajectory files")

            # Get full paths to trajectory files
            full_paths = [os.path.join(input_dir, filename) for filename in all_tstride_files]

            # Check if files exist
            existing_paths = [path for path in full_paths if os.path.exists(path)]
            missing_files = [path for path in full_paths if not os.path.exists(path)]

            if missing_files:
                warning_msg = f"Process {process_id}: Warning: {len(missing_files)} files do not exist. First missing: {missing_files[0]}"
                logger.warning(warning_msg)
                results['errors'].append(warning_msg)

            if not existing_paths:
                error_msg = f"Process {process_id}: Error: No valid all_tstride files found for {site} on {date}"
                logger.error(error_msg)
                results['errors'].append(error_msg)
            else:
                try:
                    # Create animation
                    animator = MultiParticleAnimation(existing_paths,
                                                     particle_subsample=particle_subsample,
                                                     time_stride=time_stride)

                    # Set GPU options
                    if use_gpu and gpu_config and gpu_config['provider'] is not None:
                        animator.use_gpu = True
                        animator.gpu_provider = gpu_config['provider']
                        animator.gpu_batch_size = gpu_batch_size if gpu_batch_size else gpu_config['batch_size']
                        animator.max_workers = max_workers if max_workers else gpu_config['max_workers']
                        logger.info(f"Process {process_id}: Using GPU acceleration with {animator.gpu_provider}")
                    else:
                        animator.use_gpu = False
                        logger.info(f"Process {process_id}: Using CPU processing")

                    # If we have a global extent, use it for all videos
                    if global_extent:
                        animator.custom_extent = global_extent

                    # Get file info to extract site names for filename
                    file_info = animator.get_file_info()

                    # Extract unique site names and dates
                    site_names = sorted(list(set(info['site'] for info in file_info if info['site'])))
                    dates = sorted(list(set(info['date'] for info in file_info if info['date'])))

                    # Create concatenated site name for filename
                    concatenated_site_names = "_".join(site_names)

                    # Use date from filename if available, otherwise use config date
                    date_str = dates[0] if dates else date

                    # Create output filename with concatenated site names
                    if all_tstride_only:
                        output_file = os.path.join(output_dir, f"{concatenated_site_names}_{date_str}.mp4")
                    else:
                        output_file = os.path.join(output_dir, f"{concatenated_site_names}_{date_str}_alltstride.mp4")

                    # Create parent directory if it doesn't exist
                    os.makedirs(os.path.dirname(output_file), exist_ok=True)

                    # Skip if the file already exists
                    if os.path.exists(output_file):
                        logger.info(f"Process {process_id}: Video already exists: {output_file}")
                        results['all_tstride_success'] = True
                    else:
                        success, file_info = animator.create_animation(output_file=output_file, fps=fps,
                                                                     max_frames=max_frames,
                                                                     reverse=reverse)

                        if success:
                            logger.info(f"Process {process_id}: Successfully created all_tstride video: {output_file}")
                            results['all_tstride_success'] = True

                            # Create metadata file
                            metadata_file = output_file.replace('.mp4', '_metadata.json')
                            metadata = {
                                'video_file': os.path.basename(output_file),
                                'creation_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                'gpu_used': animator.use_gpu if hasattr(animator, 'use_gpu') else False,
                                'gpu_provider': animator.gpu_provider if hasattr(animator, 'gpu_provider') else None,
                                'files_used': [
                                    {
                                        'filename': os.path.basename(info['path']),
                                        'site': info['site'],
                                        'date': info['date'] if info['date'] else '',
                                        'n_particles': info['n_particles'],
                                        'time_steps': info['time_steps']
                                    } for info in file_info
                                ]
                            }

                            with open(metadata_file, 'w') as f:
                                json.dump(metadata, f, indent=2)

                            logger.info(f"Process {process_id}: Created metadata file: {metadata_file}")
                            results['all_tstride_metadata'] = metadata
                        else:
                            error_msg = f"Process {process_id}: Failed to create all_tstride video for {site} on {date}"
                            logger.error(error_msg)
                            results['errors'].append(error_msg)
                except Exception as e:
                    error_msg = f"Process {process_id}: Error creating all_tstride video for {site} on {date}: {str(e)}"
                    logger.error(error_msg)
                    results['errors'].append(error_msg)

                # Clean up memory
                del animator
                gc.collect()

        if not regular_files and not all_tstride_files:
            warning_msg = f"Process {process_id}: No trajectory files found for {site} on {date}"
            logger.warning(warning_msg)
            results['errors'].append(warning_msg)

        # Force garbage collection after each site to minimize memory usage
        gc.collect()

        logger.info(f"Process {process_id}: Completed {site} on {date}")
        return results

    except Exception as e:
        logger.error(f"Process {process_id}: Unhandled error processing {config_key}: {str(e)}")
        return {
            'site': config.get('site', 'unknown'),
            'date': config.get('date', 'unknown'),
            'regular_success': False,
            'all_tstride_success': False,
            'errors': [f"Unhandled error: {str(e)}"],
            'regular_metadata': None,
            'all_tstride_metadata': None,
            'gpu_used': use_gpu,
            'gpu_provider': gpu_config['provider'] if use_gpu and gpu_config else None
        }


def calculate_global_extent(configs_to_process, input_dir, particle_subsample, time_stride):
    """Calculate a global extent that can be shared across all videos."""
    logger.info("Calculating overall map extent for consistent videos...")
    # Sample a few files from each config to calculate extent
    sampled_files = []
    for config_key, config in configs_to_process:
        # Take at most 2 files from each config
        for filename in config['trajectory_files'][:2]:
            filepath = os.path.join(input_dir, filename)
            if os.path.exists(filepath):
                sampled_files.append(filepath)

    # Create a temporary animator to calculate bounds
    if sampled_files:
        try:
            temp_animator = MultiParticleAnimation(sampled_files,
                                                   particle_subsample=particle_subsample,
                                                   time_stride=time_stride)
            global_extent = temp_animator.get_data_extent()
            logger.info(f"Global map extent: {global_extent}")

            # Clean up memory
            del temp_animator
            gc.collect()
            return global_extent
        except Exception as e:
            logger.error(f"Error calculating global extent: {str(e)}")

    return None


def create_videos_parallel(config_dir, input_dir, output_dir, site_filter=None, max_videos=None,
                           max_frames=None, consistent_map=True, particle_subsample=1, time_stride=1,
                           all_tstride_only=False, num_processes=None, fps=10, reverse=False,
                           skip_existing=True):
    """
    Create videos for sites based on configuration, using parallel processing.

    Args:
        config_dir (str): Directory containing configuration files
        input_dir (str): Directory containing trajectory files
        output_dir (str): Directory to save videos
        site_filter (list): List of sites to process (if None, process all)
        max_videos (int): Maximum number of videos to create (if None, create all)
        max_frames (int): Target number of frames per video (if None, use all available)
        consistent_map (bool): Whether to use consistent map dimensions across all videos
        particle_subsample (int): Only use every Nth particle to reduce memory usage
        time_stride (int): Only use every Nth time step to reduce memory usage
        all_tstride_only (bool): If True, only process sites that have all_tstride files and only use those files
        num_processes (int): Number of parallel processes to use. If None, use CPU count - 1
        fps (int): Frames per second in output videos
        reverse (bool): Whether to reverse animation playback direction
        skip_existing (bool): Whether to skip creating videos that already exist
    """
    # Set number of processes if not specified
    if num_processes is None:
        num_processes = max(1, multiprocessing.cpu_count() - 1)

    # Load configurations
    with open(os.path.join(config_dir, "video_configs.json"), "r") as f:
        video_configs = json.load(f)

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Filter configurations if site_filter is provided
    if site_filter:
        video_configs = {k: v for k, v in video_configs.items()
                         if any(site in k for site in site_filter)}

    # If all_tstride_only is True, filter out configurations that don't have all_tstride files
    if all_tstride_only:
        filtered_configs = {}
        for key, config in video_configs.items():
            all_tstride_files = [f for f in config['trajectory_files'] if "all_tstride" in f]
            if all_tstride_files:  # Only keep if there are all_tstride files
                # Modify config to ONLY use all_tstride files
                config_copy = config.copy()
                config_copy['trajectory_files'] = all_tstride_files
                filtered_configs[key] = config_copy

        video_configs = filtered_configs
        logger.info(f"Filtered to {len(video_configs)} configurations with all_tstride files")

    # Prepare configurations to process
    configs_to_process = list(video_configs.items())

    # Check for existing videos and skip them if requested
    if skip_existing:
        configs_to_process = check_existing_videos(configs_to_process, output_dir, all_tstride_only)
        if not configs_to_process:
            logger.info("All videos already exist. Nothing to do.")
            return

    # Limit the number of videos if max_videos is provided
    if max_videos is not None and max_videos < len(configs_to_process):
        configs_to_process = configs_to_process[:max_videos]

    logger.info(
        f"Creating videos for {len(configs_to_process)} site/date combinations using {num_processes} processes...")

    # Calculate global extent if using consistent map dimensions
    global_extent = None
    if consistent_map and configs_to_process:
        global_extent = calculate_global_extent(configs_to_process, input_dir, particle_subsample, time_stride)

    # Create a Pool of worker processes
    start_time = time.time()
    with multiprocessing.Pool(processes=num_processes) as pool:
        # Create a partial function with fixed parameters
        process_func = partial(
            process_single_config,
            input_dir=input_dir,
            output_dir=output_dir,
            global_extent=global_extent,
            particle_subsample=particle_subsample,
            time_stride=time_stride,
            max_frames=max_frames,
            all_tstride_only=all_tstride_only,
            fps=fps,
            reverse=reverse
        )

        # Map the function to all configuration items and collect results
        results = pool.map(process_func, configs_to_process)

    # Process and report results
    end_time = time.time()
    total_time = end_time - start_time

    # Count successes and failures
    total_videos = len(results)
    regular_successes = sum(1 for r in results if r['regular_success'])
    all_tstride_successes = sum(1 for r in results if r['all_tstride_success'])
    failures = sum(1 for r in results if not (r['regular_success'] or r['all_tstride_success']))

    logger.info("\n" + "=" * 50)
    logger.info(f"Processing complete! Total time: {total_time:.2f} seconds")
    logger.info(f"Successfully created {regular_successes + all_tstride_successes} videos")
    logger.info(f"Failed to create {failures} videos")
    logger.info("=" * 50)

    # Save results to a JSON file
    results_file = os.path.join(output_dir, "video_creation_results.json")
    with open(results_file, 'w') as f:
        json.dump({
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'total_time_seconds': total_time,
            'total_configs': total_videos,
            'regular_successes': regular_successes,
            'all_tstride_successes': all_tstride_successes,
            'failures': failures,
            'details': results
        }, f, indent=2)

    logger.info(f"Results saved to {results_file}")
    logger.info("Done!")


def check_existing_videos(configs_to_process, output_dir, all_tstride_only=False):
    """
    Check which videos already exist and filter out those configurations.

    Args:
        configs_to_process: List of (config_key, config_dict) tuples
        output_dir: Directory where videos are saved
        all_tstride_only: If True, only check for all_tstride videos

    Returns:
        List of configurations that don't have existing videos
    """
    logger.info("Checking for existing videos...")
    configs_to_create = []
    existing_videos = 0

    for config_key, config in configs_to_process:
        # Get site and date information
        site = config['site']
        date = config['date']

        # Separate regular files from all_tstride files
        if all_tstride_only:
            all_tstride_files = config['trajectory_files']  # All files are all_tstride
            regular_files = []  # No regular files
        else:
            regular_files = [f for f in config['trajectory_files'] if "all_tstride" not in f]
            all_tstride_files = [f for f in config['trajectory_files'] if "all_tstride" in f]

        # Determine if we need to create videos for this configuration
        need_to_create = False

        # Check for regular video first (if applicable)
        if regular_files and not all_tstride_only:
            # Extract site names from filenames for more accurate filename prediction
            site_names = set()
            for filename in regular_files:
                if 'GB1' in filename:
                    gb_match = re.search(r'GB1_([A-Z])', filename)
                    if gb_match:
                        site_names.add(f"GB1_{gb_match.group(1)}")
                elif 'GB2' in filename:
                    gb_match = re.search(r'GB2_([A-Z])', filename)
                    if gb_match:
                        site_names.add(f"GB2_{gb_match.group(1)}")
                elif 'GB3' in filename:
                    gb_match = re.search(r'GB3_([A-Z])', filename)
                    if gb_match:
                        site_names.add(f"GB3_{gb_match.group(1)}")
                else:
                    site_match = re.search(r'tier2_site_(\d+)', filename)
                    if site_match:
                        site_names.add(f"Tier2_Site_{site_match.group(1)}")

            # If we couldn't extract site names, use the site from config
            if not site_names:
                site_names = {site}

            # Create concatenated site name for filename
            concatenated_site_names = "_".join(sorted(list(site_names)))

            # Check both date formats that might appear in filenames
            possible_dates = [date]
            date_match = re.search(r'_(\d{8})_', regular_files[0]) if regular_files else None
            if date_match and date_match.group(1) not in possible_dates:
                possible_dates.append(date_match.group(1))

            # Check all possible output files
            regular_exists = False
            for date_str in possible_dates:
                output_file = os.path.join(output_dir, f"{concatenated_site_names}_{date_str}.mp4")
                if os.path.exists(output_file):
                    regular_exists = True
                    break

            if not regular_exists:
                need_to_create = True

        # Check for all_tstride video
        if all_tstride_files:
            # Extract site names from filenames
            site_names = set()
            for filename in all_tstride_files:
                if 'GB1' in filename:
                    gb_match = re.search(r'GB1_([A-Z])', filename)
                    if gb_match:
                        site_names.add(f"GB1_{gb_match.group(1)}")
                elif 'GB2' in filename:
                    gb_match = re.search(r'GB2_([A-Z])', filename)
                    if gb_match:
                        site_names.add(f"GB2_{gb_match.group(1)}")
                elif 'GB3' in filename:
                    gb_match = re.search(r'GB3_([A-Z])', filename)
                    if gb_match:
                        site_names.add(f"GB3_{gb_match.group(1)}")
                else:
                    site_match = re.search(r'tier2_site_(\d+)', filename)
                    if site_match:
                        site_names.add(f"Tier2_Site_{site_match.group(1)}")

            # If we couldn't extract site names, use the site from config
            if not site_names:
                site_names = {site}

            # Create concatenated site name for filename
            concatenated_site_names = "_".join(sorted(list(site_names)))

            # Check both date formats that might appear in filenames
            possible_dates = [date]
            date_match = re.search(r'_(\d{8})_', all_tstride_files[0]) if all_tstride_files else None
            if date_match and date_match.group(1) not in possible_dates:
                possible_dates.append(date_match.group(1))

            # Check all possible output files
            all_tstride_exists = False
            for date_str in possible_dates:
                if all_tstride_only:
                    output_file = os.path.join(output_dir, f"{concatenated_site_names}_{date_str}.mp4")
                else:
                    output_file = os.path.join(output_dir, f"{concatenated_site_names}_{date_str}_alltstride.mp4")

                if os.path.exists(output_file):
                    all_tstride_exists = True
                    break

            if not all_tstride_exists:
                need_to_create = True

        # Only add configuration if we need to create a video
        if need_to_create:
            configs_to_create.append((config_key, config))
        else:
            existing_videos += 1

    logger.info(f"Found {existing_videos} existing videos, {len(configs_to_create)} videos need to be created")
    return configs_to_create


def main():
    parser = argparse.ArgumentParser(description='Create videos for sites (parallel GPU-accelerated version)')
    parser.add_argument('--config-dir', type=str, required=True,
                        help='Directory containing configuration files')
    parser.add_argument('--input-dir', type=str, required=True,
                        help='Directory containing trajectory files')
    parser.add_argument('--output-dir', type=str, required=True,
                        help='Directory to save videos')
    parser.add_argument('--site-filter', type=str, nargs='+',
                        help='Filter sites to process (e.g., GB1_A GB2_B)')
    parser.add_argument('--max-videos', type=int,
                        help='Maximum number of videos to create')

    # Frame control options
    parser.add_argument('--frames', type=int, default=None,
                        help='Target number of frames per video (default: use all available frames)')
    parser.add_argument('--stride', type=int, default=1,
                        help='Use every Nth time step (default: 1, use all frames)')

    # Map and particle options
    parser.add_argument('--consistent-map', action='store_true', default=True,
                        help='Use consistent map dimensions across all videos')
    parser.add_argument('--particle-subsample', type=int, default=1,
                        help='Only use every Nth particle to reduce memory (default: 1, use all)')
    parser.add_argument('--all-tstride-only', action='store_true',
                        help='Only process sites with all_tstride files and only use those files')

    # Processing options
    parser.add_argument('--num-processes', type=int,
                        help='Number of parallel processes to use (default: CPU count - 1)')
    parser.add_argument('--fps', type=int, default=10,
                        help='Frames per second in output video (default: 10)')
    parser.add_argument('--reverse', action='store_true', default=True,
                        help='Reverse the direction of animation playback')
    parser.add_argument('--skip-existing', action='store_true', default=True,
                        help='Skip creating videos that already exist (default: True)')

    # GPU acceleration options
    parser.add_argument('--use-gpu', action='store_true', default=True,
                        help='Use GPU acceleration if available')
    parser.add_argument('--gpu-batch-size', type=int, default=None,
                        help='Number of frames to process in parallel on GPU')
    parser.add_argument('--gpu-provider', type=str, choices=['auto', 'cupy', 'torch', 'none'], default='auto',
                        help='GPU acceleration provider to use (default: auto-detect)')
    parser.add_argument('--max-workers', type=int, default=None,
                        help='Maximum number of worker threads for frame rendering (default: auto)')

    args = parser.parse_args()

    logger.info("Particle Animation Creator starting...")
    logger.info(f"Config directory: {args.config_dir}")
    logger.info(f"Input directory: {args.input_dir}")
    logger.info(f"Output directory: {args.output_dir}")
    logger.info(f"Skip existing videos: {args.skip_existing}")

    if args.site_filter:
        logger.info(f"Site filter: {args.site_filter}")

    if args.max_videos:
        logger.info(f"Maximum videos: {args.max_videos}")

    if args.frames:
        logger.info(f"Target frames per video: {args.frames}")
    logger.info(f"Time stride: {args.stride}")
    logger.info(f"Consistent map: {args.consistent_map}")
    logger.info(f"Particle subsample: {args.particle_subsample}")
    logger.info(f"All tstride only: {args.all_tstride_only}")
    logger.info(f"FPS: {args.fps}")
    logger.info(f"Reverse playback: {args.reverse}")

    # GPU configuration
    gpu_config = detect_gpu_capabilities(args.gpu_provider)
    use_gpu = args.use_gpu and gpu_config['provider'] is not None

    if use_gpu:
        logger.info(f"GPU acceleration enabled: {gpu_config['provider']}")
        logger.info(f"GPU batch size: {args.gpu_batch_size if args.gpu_batch_size else gpu_config['batch_size']}")
        logger.info(f"GPU workers: {args.max_workers if args.max_workers else gpu_config['max_workers']}")
    else:
        logger.info("GPU acceleration disabled")

    # CPU process configuration
    if args.num_processes:
        num_processes = args.num_processes
        logger.info(f"Number of CPU processes: {num_processes}")
    else:
        num_processes = max(1, multiprocessing.cpu_count() - 1)
        logger.info(f"Number of CPU processes: {num_processes} (auto)")

    # Load configurations
    with open(os.path.join(args.config_dir, "video_configs.json"), "r") as f:
        video_configs = json.load(f)

    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)

    # Filter configurations if site_filter is provided
    if args.site_filter:
        video_configs = {k: v for k, v in video_configs.items()
                         if any(site in k for site in args.site_filter)}

    # If all_tstride_only is True, filter configurations
    if args.all_tstride_only:
        filtered_configs = {}
        for key, config in video_configs.items():
            all_tstride_files = [f for f in config['trajectory_files'] if "all_tstride" in f]
            if all_tstride_files:
                config_copy = config.copy()
                config_copy['trajectory_files'] = all_tstride_files
                filtered_configs[key] = config_copy

        video_configs = filtered_configs
        logger.info(f"Filtered to {len(video_configs)} configurations with all_tstride files")

    # Prepare configurations to process
    configs_to_process = list(video_configs.items())

    # Check for existing videos and skip them if requested
    if args.skip_existing:
        configs_to_process = check_existing_videos(configs_to_process, args.output_dir, args.all_tstride_only)
        if not configs_to_process:
            logger.info("All videos already exist. Nothing to do.")
            return

    # Limit the number of videos if max_videos is provided
    if args.max_videos is not None and args.max_videos < len(configs_to_process):
        configs_to_process = configs_to_process[:args.max_videos]

    logger.info(f"Creating videos for {len(configs_to_process)} site/date combinations")

    # Calculate global extent if using consistent map dimensions
    global_extent = None
    if args.consistent_map and configs_to_process:
        global_extent = calculate_global_extent(configs_to_process, args.input_dir,
                                                args.particle_subsample, args.stride)

    # Start timing
    start_time = time.time()

    # Update the process_single_config function to include GPU options
    process_func = partial(
        process_single_config,
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        global_extent=global_extent,
        particle_subsample=args.particle_subsample,
        time_stride=args.stride,
        max_frames=args.frames,
        all_tstride_only=args.all_tstride_only,
        fps=args.fps,
        reverse=args.reverse,
        use_gpu=use_gpu,
        gpu_config=gpu_config,
        gpu_batch_size=args.gpu_batch_size if args.gpu_batch_size else gpu_config['batch_size'],
        max_workers=args.max_workers if args.max_workers else gpu_config['max_workers']
    )

    # Execute parallel processing
    with multiprocessing.Pool(processes=num_processes) as pool:
        results = pool.map(process_func, configs_to_process)

    # End timing
    end_time = time.time()
    total_time = end_time - start_time

    # Count successes and failures
    total_videos = len(results)
    regular_successes = sum(1 for r in results if r.get('regular_success', False))
    all_tstride_successes = sum(1 for r in results if r.get('all_tstride_success', False))
    failures = sum(1 for r in results if not (r.get('regular_success', False) or r.get('all_tstride_success', False)))

    logger.info("\n" + "=" * 50)
    logger.info(f"Processing complete! Total time: {total_time:.2f} seconds")
    logger.info(f"Successfully created {regular_successes + all_tstride_successes} videos")
    logger.info(f"Failed to create {failures} videos")
    logger.info("=" * 50)

    # Save results to a JSON file
    results_file = os.path.join(args.output_dir, "video_creation_results.json")
    with open(results_file, 'w') as f:
        json.dump({
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'total_time_seconds': total_time,
            'total_configs': total_videos,
            'regular_successes': regular_successes,
            'all_tstride_successes': all_tstride_successes,
            'failures': failures,
            'gpu_acceleration': use_gpu,
            'gpu_provider': gpu_config['provider'] if use_gpu else None,
            'details': results
        }, f, indent=2)

    logger.info(f"Results saved to {results_file}")
    logger.info("Done!")


# Detect GPU capabilities function
def detect_gpu_capabilities(provider='auto'):
    """Detect GPU capabilities and adjust settings accordingly."""
    if provider == 'none':
        logger.info("GPU acceleration explicitly disabled")
        return {'provider': None, 'batch_size': 5, 'max_workers': 4}

    # Try specified provider first
    if provider in ['cupy', 'torch']:
        if provider == 'cupy':
            return detect_cupy_capabilities()
        else:
            return detect_torch_capabilities()

    # Auto-detect
    try:
        # Try CuPy first (NVIDIA GPUs)
        return detect_cupy_capabilities()
    except:
        try:
            # Try PyTorch (works on more GPUs)
            return detect_torch_capabilities()
        except:
            pass

    # No GPU or detection failed
    logger.warning("No suitable GPU detected or GPU libraries not available")
    return {'provider': None, 'batch_size': 5, 'max_workers': 4}


def detect_cupy_capabilities():
    """Detect CuPy (CUDA) capabilities."""
    try:
        import cupy as cp
        gpu_info = cp.cuda.runtime.getDeviceProperties(0)
        logger.info(f"CUDA GPU detected: {gpu_info['name'].decode('utf-8')}")

        memory_mb = gpu_info['totalGlobalMem'] / (1024 * 1024)
        logger.info(f"GPU memory: {memory_mb:.2f} MB")

        # Adjust settings based on GPU capability
        if memory_mb > 8000:  # High-end GPU with >8GB
            return {
                'provider': 'cupy',
                'batch_size': 50,
                'max_workers': 16
            }
        elif memory_mb > 4000:  # Mid-range GPU with >4GB
            return {
                'provider': 'cupy',
                'batch_size': 20,
                'max_workers': 8
            }
        else:  # Low-end GPU
            return {
                'provider': 'cupy',
                'batch_size': 10,
                'max_workers': 4
            }
    except Exception as e:
        logger.warning(f"CuPy detection failed: {str(e)}")
        raise


def detect_torch_capabilities():
    """Detect PyTorch capabilities."""
    try:
        import torch
        if torch.cuda.is_available():
            device_name = torch.cuda.get_device_name(0)
            memory_mb = torch.cuda.get_device_properties(0).total_memory / (1024 * 1024)
            logger.info(f"PyTorch CUDA GPU detected: {device_name}")
            logger.info(f"GPU memory: {memory_mb:.2f} MB")

            return {
                'provider': 'torch',
                'batch_size': max(5, int(memory_mb / 500)),  # Rough estimate
                'max_workers': 4
            }
        else:
            raise RuntimeError("PyTorch reports no CUDA device available")
    except Exception as e:
        logger.warning(f"PyTorch detection failed: {str(e)}")
        raise




if __name__ == "__main__":
    # Python's multiprocessing works best with the "spawn" method on all platforms
    multiprocessing.set_start_method('spawn', force=True)
    main()
