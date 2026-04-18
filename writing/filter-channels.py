#!/usr/bin/env python3
# Author: Macon Abernathy

import numpy as np
from typing import List, Tuple, Dict
import re
import matplotlib.pyplot as plt
import argparse
import os
import json
from dataclasses import dataclass
from datetime import datetime, timezone
import dataclasses

@dataclass
class ProcessingConfig:
    # Main filtering parameters
    outlier_threshold: float = 30.0
    iterations: int = 2
    normalize_before_correlation: bool = True  # Add this line
    
    # Channel quality thresholds
    correlation_threshold: float = 0.95
    flat_channel_threshold: float = 1e-4
    
    # Processing parameters
    window_size_fraction: float = 0.05  # For rolling window (as fraction of data length)
    min_window_length: int = 5
    max_window_length: int = 21
    
    # Smoothing options
    use_smoothing: bool = False  # Default to current v25 behavior
    
    # Savitzky-Golay filter parameters
    sg_polynomial_order: int = 3
    
class XASDataProcessor:
    def __init__(self, filename: str, config: ProcessingConfig):
        self.filename = filename
        self.config = config
        self.weights = []
        self.data_columns = {}
        self.ff_columns = {}
        self.column_names = []
        self.energy = None
        
    def is_fluorescence_column(self, name: str) -> bool:
        """Check if a column name represents a fluorescence detector channel."""
        return (name.startswith('FF') or 
                name.startswith('SCA1_'))

    def read_file(self) -> None:
        """Read and parse the SSRL XAS data file."""
        with open(self.filename, 'r') as file:
            content = file.read()
            
            # Split at Data: marker and get the section after it
            if 'Data:' not in content:
                raise ValueError("Could not find 'Data:' marker in file")
            
            data_section = content.split('Data:')[1].strip()
            
            # Split into lines and remove empty lines
            lines = [line.strip() for line in data_section.split('\n') if line.strip()]
            
            # Find where the actual data starts (first line with numbers)
            data_start_idx = 0
            for i, line in enumerate(lines):
                if re.match(r'^[\d.-]', line):
                    data_start_idx = i
                    break
            
            # Column names are all lines before the data
            # Keep the entire line (removing only trailing whitespace)
            self.column_names = [line.rstrip() for line in lines[:data_start_idx]]
            
            print(f"\nFound {len(self.column_names)} columns")
            
            # Process data lines
            data_lines = lines[data_start_idx:]
            
            try:
                # Convert data lines to numpy array
                data_array = np.zeros((len(data_lines), len(self.column_names)))
                for i, line in enumerate(data_lines):
                    values = line.split()
                    if len(values) != len(self.column_names):
                        print(f"Warning: Line {i} has {len(values)} values but expected {len(self.column_names)}")
                        continue
                    data_array[i] = [float(x) for x in values]
                
                # Try Achieved Energy first, then Requested Energy
                energy_col = None
                energy_col_idx = None
                
                if 'Achieved Energy' in self.column_names:
                    energy_col = 'Achieved Energy'
                    energy_col_idx = self.column_names.index(energy_col)
                    print("\nUsing 'Achieved Energy' column for energy values")
                elif 'Requested Energy' in self.column_names:
                    energy_col = 'Requested Energy'
                    energy_col_idx = self.column_names.index(energy_col)
                    print("\nUsing 'Requested Energy' column for energy values")
                else:
                    raise ValueError("Could not find either 'Achieved Energy' or 'Requested Energy' column")
                
                self.energy = data_array[:, energy_col_idx]
                
                # Create data columns dictionary and extract fluorescence channels
                fluorescence_channels = []
                for i, name in enumerate(self.column_names):
                    self.data_columns[name] = data_array[:, i]
                    if self.is_fluorescence_column(name):
                        self.ff_columns[name] = data_array[:, i]
                        fluorescence_channels.append(name)
                
                print("\nFound fluorescence channels:")
                for channel in fluorescence_channels:
                    print(f"  - {channel}")
                
                if not self.ff_columns:
                    print("\nWarning: No fluorescence channels found in the data")
                    
            except Exception as e:
                raise ValueError(f"Error processing data: {str(e)}")
                
            print(f"\nSuccessfully loaded data with {len(data_lines)} points")

    def identify_bad_channels(self) -> List[str]:
            """Identify bad FF channels through multiple iterations with improved filtering logic."""
            if not self.ff_columns:
                raise ValueError("No FF channels found in the data")
                
            def normalize_channel(data):
                """Normalize channel data to [0,1] range."""
                min_val = np.min(data)
                max_val = np.max(data)
                if max_val == min_val:
                    return np.zeros_like(data)
                return (data - min_val) / (max_val - min_val)
                
            bad_channels = set()
            
            # First, identify and remove flat/zero channels
            for name, data in self.ff_columns.items():
                n_regions = 4
                region_size = len(data) // n_regions
                region_stats = []
                
                for i in range(n_regions):
                    start_idx = i * region_size
                    end_idx = start_idx + region_size
                    region_data = data[start_idx:end_idx]
                    region_stats.append({
                        'mean': np.mean(region_data),
                        'std': np.std(region_data)
                    })
                
                means = [stats['mean'] for stats in region_stats]
                stds = [stats['std'] for stats in region_stats]
                
                if (all(std < self.config.flat_channel_threshold * np.mean(means) for std in stds) or
                    np.std(means) < self.config.flat_channel_threshold * np.mean(means)):
                    bad_channels.add(name)
                    continue
            
            remaining_channels = {
                name: data 
                for name, data in self.ff_columns.items() 
                if name not in bad_channels
            }
            
            # Process remaining channels through iterations
            for iteration in range(self.config.iterations):
                if not remaining_channels:
                    break
                        
                # Calculate median profile from remaining channels
                if self.config.normalize_before_correlation:
                    arrays = np.array([normalize_channel(data) for data in remaining_channels.values()])
                else:
                    arrays = np.array(list(remaining_channels.values()))
                median_profile = np.median(arrays, axis=0)
                
                # If smoothing is enabled, apply it to the median profile
                if self.config.use_smoothing:
                    window_length = min(
                        self.config.max_window_length,
                        max(self.config.min_window_length, len(median_profile) // 20)
                    )
                    window_length = window_length + 1 if window_length % 2 == 0 else window_length
                    median_profile = self._savitzky_golay(median_profile, window_length, self.config.sg_polynomial_order)
                
                for name in list(remaining_channels.keys()):
                    data = remaining_channels[name]
                    
                    # If smoothing is enabled, smooth the current channel data
                    if self.config.use_smoothing:
                        window_length = min(
                            self.config.max_window_length,
                            max(self.config.min_window_length, len(data) // 20)
                        )
                        window_length = window_length + 1 if window_length % 2 == 0 else window_length
                        data = self._savitzky_golay(data, window_length, self.config.sg_polynomial_order)
                    
                    # Calculate correlation with median profile
                    if self.config.normalize_before_correlation:
                        correlation = np.corrcoef(normalize_channel(data), normalize_channel(median_profile))[0, 1]
                    else:
                        correlation = np.corrcoef(data, median_profile)[0, 1]
                    
                    
                    # Calculate normalized residuals
                    if self.config.normalize_before_correlation:
                        normalized_data = normalize_channel(data)
                        normalized_median = normalize_channel(median_profile)
                        residuals = normalized_data - normalized_median
                        mad = np.median(np.abs(arrays - normalized_median), axis=1)
                        normalized_residuals = residuals / (np.median(mad) + 1e-10)
                    else:
                        residuals = data - median_profile
                        mad = np.median(np.abs(arrays - median_profile), axis=1)
                        normalized_residuals = residuals / (np.median(mad) + 1e-10)
                    
                    # Calculate local variation
                    # Use normalized data when normalization is enabled,
                    # so the metric is scale-invariant (works on both raw
                    # and dead-time-corrected data)
                    window_size = max(
                        self.config.min_window_length,
                        min(
                            self.config.max_window_length,
                            int(len(data) * self.config.window_size_fraction)
                        )
                    )
                    variation_data = normalize_channel(data) if self.config.normalize_before_correlation else data
                    rolling_std = np.array([
                        np.std(variation_data[i:i+window_size])
                        for i in range(len(variation_data)-window_size)
                    ])
                    
                    # Apply criteria
                    is_bad = False
                    reasons = []
                    
                    if correlation < self.config.correlation_threshold:
                        is_bad = True
                        reasons.append(f"Low correlation with median profile ({correlation:.3f})")
                    
                    max_abs_residual = np.max(np.abs(normalized_residuals))
                    if max_abs_residual > self.config.outlier_threshold:
                        is_bad = True
                        reasons.append(f"Large deviation from median profile ({max_abs_residual:.2f})")
                    
                    std_ratio = np.max(rolling_std) / (np.mean(rolling_std) + 1e-10)
                    if std_ratio > self.config.outlier_threshold:
                        is_bad = True
                        reasons.append(f"Excessive local variation ({std_ratio:.2f})")
                    
                    if is_bad:
                        bad_channels.add(name)
                        print(f"Channel {name} marked as bad:")
                        for reason in reasons:
                            print(f"  - {reason}")
                
                # Update remaining channels
                remaining_channels = {
                    name: data 
                    for name, data in remaining_channels.items() 
                    if name not in bad_channels
                }
                
                if iteration < self.config.iterations - 1 and len(remaining_channels) < len(self.ff_columns):
                    print(f"\nIteration {iteration + 1} complete, {len(bad_channels)} total bad channels found")
                    print("Recalculating without bad channels...\n")
            
            return sorted(list(bad_channels))

    def _savitzky_golay(self, y, window_length, poly_order):
        """
        Apply Savitzky-Golay filter to reduce noise while preserving signal features.
        """
        try:
            from scipy.signal import savgol_filter
            return savgol_filter(y, window_length, poly_order)
        except ImportError:
            # Fallback to simple moving average if scipy is not available
            print("Warning: scipy not available, falling back to simple moving average")
            window = np.ones(window_length) / window_length
            return np.convolve(y, window, mode='same')

    def get_good_channels(self, bad_channels: List[str]) -> Dict[str, np.ndarray]:
        """Return dictionary of good FF channels."""
        return {
            name: data 
            for name, data in self.ff_columns.items() 
            if name not in bad_channels
        }

    def plot_spectra(self, bad_channels: List[str], save_path: str = None):
        """Plot the original and filtered spectra."""
        if not self.ff_columns:
            raise ValueError("No FF channels to plot")
            
        # Create figure with subplots
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
        
        # Plot 1: All channels
        ax1.set_title('All FF Channels')
        for name, data in self.ff_columns.items():
            if name in bad_channels:
                ax1.plot(self.energy, data, 'r-', alpha=0.3, linewidth=1)
            else:
                ax1.plot(self.energy, data, 'b-', alpha=0.3, linewidth=1)
        
        # Add legend proxy artists
        ax1.plot([], [], 'b-', label='Good Channels')
        ax1.plot([], [], 'r-', label='Bad Channels')
        ax1.legend()
        ax1.set_xlabel('Energy (eV)')
        ax1.set_ylabel('Intensity')
        
        # Plot 2: Good channels only
        ax2.set_title('Good FF Channels Only')
        good_channels = self.get_good_channels(bad_channels)
        for data in good_channels.values():
            ax2.plot(self.energy, data, 'b-', alpha=0.3, linewidth=1)
        
        # Plot average of good channels
        if good_channels:
            avg_data = np.mean(list(good_channels.values()), axis=0)
            ax2.plot(self.energy, avg_data, 'k-', linewidth=2, label='Average')
        
        ax2.legend()
        ax2.set_xlabel('Energy (eV)')
        ax2.set_ylabel('Intensity')
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path)
            plt.close()
        else:
            plt.show()

    def has_ref_in_spare(self) -> bool:
        """Check if the 'spare' column contains meaningful (non-zero) reference foil data."""
        if 'spare' not in self.data_columns:
            return False
        spare = self.data_columns['spare']
        # Consider it real data if more than 10% of values are non-zero
        # and the mean is well above noise
        nonzero_frac = np.count_nonzero(spare) / len(spare)
        return nonzero_frac > 0.1 and np.mean(np.abs(spare)) > 1.0

    def save_bcr_sidecar(self, output_file: str, bad_channels: List[str],
                         pre_excluded: List[str], ref_mode: str,
                         script_version: str = "3.0") -> str:
        """Write a .bcr.json sidecar recording config and filter results."""
        sidecar_path = output_file + ".bcr.json"

        payload = {
            "bcr_script_version": script_version,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "source_file": os.path.basename(self.filename),
            "output_file": os.path.basename(output_file),
            "config": dataclasses.asdict(self.config),
            "pre_excluded_channels": sorted(pre_excluded),
            "bad_channels_removed": sorted(bad_channels),
            "ref_mode_used": ref_mode,
            "notes": "",
        }

        with open(sidecar_path, 'w') as f:
            json.dump(payload, f, indent=2)

        print(f"Config sidecar saved to: {sidecar_path}")
        return sidecar_path

    def save_processed_data(self, bad_channels: List[str], ref_mode: str = 'auto',
                            pre_excluded: List[str] = None):
        """
        Save processed data to a new file with BCR_ prefix.
        Includes: Energy, I0, I1, I2, sum of good FF channels,
        and optionally the reference foil spectrum.
        
        ref_mode: 'auto' (detect spare), 'spare' (force log(I0/spare)),
                  'transmission' (force log(I1/I2)), or 'none'
        """
        # Create output filename by adding BCR_ prefix to original filename
        base_name = os.path.basename(self.filename)
        output_file = os.path.join(os.path.dirname(self.filename), 'BCR_' + base_name)
        
        # Get good channels and calculate their sum
        good_channels = self.get_good_channels(bad_channels)
        good_channels_sum = np.sum(list(good_channels.values()), axis=0)
        
        # Get required columns using the same energy source as was used in processing
        if 'Achieved Energy' in self.data_columns:
            energy = self.data_columns['Achieved Energy']
            print("Using 'Achieved Energy' in output file")
        else:
            energy = self.data_columns['Requested Energy']
            print("Using 'Requested Energy' in output file")

        i0 = self.data_columns['I0']
        i1 = self.data_columns['I1']
        i2 = self.data_columns['I2']
        
        # Determine reference foil spectrum
        include_ref = False
        ref_label = None
        ref_data = None
        
        if ref_mode == 'auto':
            if self.has_ref_in_spare():
                ref_mode = 'spare'
                print("Auto-detected reference foil data in 'spare' column")
            else:
                ref_mode = 'none'
        
        if ref_mode == 'spare':
            spare = self.data_columns.get('spare')
            if spare is not None:
                # Avoid log(0) or log(negative)
                safe_spare = np.where(spare > 0, spare, np.nan)
                safe_i0 = np.where(i0 > 0, i0, np.nan)
                ref_data = np.log(safe_i0 / safe_spare)
                ref_label = 'ref_log(I0/spare)'
                include_ref = True
                print("Reference foil: log(I0/spare)")
            else:
                print("Warning: 'spare' column not found, skipping reference foil")
        
        elif ref_mode == 'transmission':
            safe_i1 = np.where(i1 > 0, i1, np.nan)
            safe_i2 = np.where(i2 > 0, i2, np.nan)
            ref_data = np.log(safe_i1 / safe_i2)
            ref_label = 'ref_log(I1/I2)'
            include_ref = True
            print("Reference foil: log(I1/I2)")
        
        # Build column header
        columns = 'Energy I0 I1 I2 FF_sum'
        if include_ref:
            columns += f' {ref_label}'
        
        # Write to file
        with open(output_file, 'w') as f:
            # Write header
            f.write("# Processed XAS data with sum of good FF channels\n")
            f.write("# Original file: {}\n".format(self.filename))
            f.write("# Number of good FF channels summed: {}\n".format(len(good_channels)))
            f.write("# Bad channels removed: {}\n".format(', '.join(bad_channels)))
            if include_ref:
                f.write("# Reference foil: {}\n".format(ref_label))
            f.write("# Columns: {}\n".format(columns))
            
            # Write data
            for i in range(len(energy)):
                line = f"{energy[i]:.6f} {i0[i]:.6f} {i1[i]:.6f} {i2[i]:.6f} {good_channels_sum[i]:.6f}"
                if include_ref:
                    ref_val = ref_data[i]
                    if np.isnan(ref_val):
                        line += " nan"
                    else:
                        line += f" {ref_val:.6f}"
                f.write(line + "\n")
        
        print(f"\nProcessed data saved to: {output_file}")
        print(f"Included sum of {len(good_channels)} good FF channels")
        if include_ref:
            print(f"Included reference foil column: {ref_label}")

        if pre_excluded is None:
            pre_excluded = []
        self.save_bcr_sidecar(
            output_file=output_file,
            bad_channels=bad_channels,
            pre_excluded=pre_excluded,
            ref_mode=ref_mode,
        )

def prompt_value(label, current, cast=float):
    """Prompt user for a value, showing current default. Enter keeps current."""
    raw = input(f"  {label} [{current}]: ").strip()
    if not raw:
        return current
    try:
        return cast(raw)
    except ValueError:
        print(f"    Invalid input, keeping {current}")
        return current


def prompt_yes_no(question, default=True):
    """Prompt for yes/no with a default."""
    suffix = "[Y/n]" if default else "[y/N]"
    raw = input(f"{question} {suffix}: ").strip().lower()
    if not raw:
        return default
    return raw in ('y', 'yes')


def interactive_config(config: ProcessingConfig) -> ProcessingConfig:
    """Walk the user through key parameters."""
    print("\n--- Filter Settings ---")
    print("  (Press Enter to keep current value)\n")
    
    config.outlier_threshold = prompt_value(
        "Outlier threshold (std devs)", config.outlier_threshold)
    config.iterations = prompt_value(
        "Filter iterations", config.iterations, cast=int)
    config.correlation_threshold = prompt_value(
        "Correlation threshold", config.correlation_threshold)
    config.window_size_fraction = prompt_value(
        "Window size fraction", config.window_size_fraction)
    
    print()
    return config


def interactive_exclude_channels() -> str:
    """Prompt for channels to pre-exclude."""
    raw = input("  Pre-exclude channels (comma-separated numbers, or Enter for none): ").strip()
    return raw if raw else None


def interactive_ref_channel() -> str:
    """Prompt for reference foil mode."""
    print("\n  Reference foil options:")
    print("    1. auto  - detect from spare column")
    print("    2. spare - force log(I0/spare)")
    print("    3. trans - force log(I1/I2)")
    print("    4. none  - skip reference foil")
    raw = input("  Choice [1]: ").strip()
    mapping = {'1': 'auto', '2': 'spare', '3': 'transmission', '4': 'none',
               'auto': 'auto', 'spare': 'spare', 'trans': 'transmission', 
               'transmission': 'transmission', 'none': 'none', '': 'auto'}
    return mapping.get(raw, 'auto')


def print_results_summary(file_path, bad_channels, pre_excluded, total_channels):
    """Print a clear results summary."""
    all_bad = sorted(set(bad_channels + pre_excluded))
    total_bad = len(all_bad)
    
    print(f"\n{'='*50}")
    print(f"  Results: {os.path.basename(file_path)}")
    print(f"{'='*50}")
    if pre_excluded:
        print(f"  Pre-excluded:  {', '.join(pre_excluded)}")
    print(f"  Bad channels:  {', '.join(bad_channels) if bad_channels else '(none)'}")
    print(f"  Total bad:     {total_bad} / {total_channels}")
    print(f"  Good channels: {total_channels - total_bad}")
    print(f"{'='*50}")


def process_single_file(file_path, config, exclude_channels, ref_channel, output_dir):
    """Process one file. Returns (processor, bad_channels, pre_excluded) or None on error."""
    processor = XASDataProcessor(file_path, config)
    
    try:
        print(f"\nReading {os.path.basename(file_path)}...")
        processor.read_file()
        
        # Pre-exclude channels
        pre_excluded = []
        if exclude_channels:
            for ch_num in exclude_channels.split(','):
                ch_num = ch_num.strip()
                ch_name = f'SCA1_{ch_num}'
                if ch_name in processor.ff_columns:
                    del processor.ff_columns[ch_name]
                    pre_excluded.append(ch_name)
                else:
                    print(f"  Warning: Channel {ch_name} not found, skipping")
            if pre_excluded:
                print(f"  Pre-excluded: {', '.join(pre_excluded)}")
        
        # Identify bad channels
        print("\nAnalyzing channels...")
        bad_channels = processor.identify_bad_channels()
        
        total_channels = len(processor.ff_columns) + len(pre_excluded)
        print_results_summary(file_path, bad_channels, pre_excluded, total_channels)
        
        # Plot
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
            plot_path = os.path.join(output_dir, f"plot_{os.path.basename(file_path)}.png")
            processor.plot_spectra(bad_channels, save_path=plot_path)
        else:
            processor.plot_spectra(bad_channels)
        
        # Save
        processor.save_processed_data(
            bad_channels,
            ref_mode=ref_channel,
            pre_excluded=pre_excluded,
        )
        
        return processor, bad_channels, pre_excluded
        
    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")
        import traceback
        traceback.print_exc()
        return None


def main():
    parser = argparse.ArgumentParser(
        description='Process XAS data files to identify and filter bad channels.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument('path',
                       help='Input .dat file or directory containing .dat files')
    parser.add_argument('--output',
                       help='Output directory for plots (default: show plots)',
                       default=None)
    parser.add_argument('--batch',
                       action='store_true',
                       help='Non-interactive batch mode (use CLI args only, no prompts)')
    
    # Filtering parameters (used as defaults in interactive mode, or directly in batch mode)
    filter_group = parser.add_argument_group('Filtering Parameters')
    filter_group.add_argument('--threshold', type=float, default=2.0,
                            help='Number of standard deviations for outlier detection')
    filter_group.add_argument('--iterations', type=int, default=3,
                            help='Number of filtering iterations')
    filter_group.add_argument('--correlation-threshold', type=float, default=0.95,
                            help='Minimum correlation coefficient for good channels')
    filter_group.add_argument('--flat-channel-threshold', type=float, default=1e-4,
                            help='Threshold for identifying flat channels')
    filter_group.add_argument('--no-normalization', action='store_false',
                            dest='normalize_before_correlation',
                            help='Disable normalization before correlation calculation')
    filter_group.add_argument('--exclude-channels', type=str, default=None,
                            help='Comma-separated channel numbers to pre-exclude')
    
    # Advanced processing parameters
    advanced_group = parser.add_argument_group('Advanced Processing Parameters')
    advanced_group.add_argument('--window-size-fraction', type=float, default=0.05,
                              help='Rolling window size as fraction of data length')
    advanced_group.add_argument('--min-window-length', type=int, default=5,
                              help='Minimum window length for smoothing')
    advanced_group.add_argument('--max-window-length', type=int, default=21,
                              help='Maximum window length for smoothing')
    advanced_group.add_argument('--sg-polynomial-order', type=int, default=3,
                              help='Polynomial order for Savitzky-Golay filter')
    
    smoothing_group = parser.add_argument_group('Smoothing Options')
    smoothing_group.add_argument('--use-smoothing', action='store_true',
                                help='Enable Savitzky-Golay smoothing before analysis')
    
    ref_group = parser.add_argument_group('Reference Foil Options')
    ref_group.add_argument('--ref-channel', type=str,
                          choices=['auto', 'spare', 'transmission', 'none'],
                          default='auto',
                          help='Reference foil source')
    
    args = parser.parse_args()
    
    # Resolve path
    input_path = os.path.abspath(os.path.expanduser(args.path))
    if not os.path.exists(input_path):
        print(f"Error: Path '{args.path}' not found.")
        return

    def is_valid_file(filename):
        if filename.endswith('.dat'):
            return True
        parts = filename.split('.')
        if len(parts) > 1:
            try:
                int(parts[-1])
                return True
            except ValueError:
                return False
        return False

    # Get file list
    if os.path.isdir(input_path):
        all_files = sorted(os.listdir(input_path))
        valid_files = [f for f in all_files if is_valid_file(f)]
        if not valid_files:
            print(f"No valid data files found in: {input_path}")
            return
        file_paths = [os.path.join(input_path, f) for f in valid_files]
        print(f"\nFound {len(file_paths)} data files")
    else:
        if not is_valid_file(os.path.basename(input_path)):
            print(f"Error: '{args.path}' is not a valid data file")
            return
        file_paths = [input_path]
    
    # Build initial config from CLI args
    config = ProcessingConfig(
        outlier_threshold=args.threshold,
        iterations=args.iterations,
        correlation_threshold=args.correlation_threshold,
        flat_channel_threshold=args.flat_channel_threshold,
        window_size_fraction=args.window_size_fraction,
        min_window_length=args.min_window_length,
        max_window_length=args.max_window_length,
        use_smoothing=args.use_smoothing,
        sg_polynomial_order=args.sg_polynomial_order,
        normalize_before_correlation=args.normalize_before_correlation
    )
    exclude_channels = args.exclude_channels
    ref_channel = args.ref_channel
    
    # ---- BATCH MODE: process everything and exit ----
    if args.batch:
        for fp in file_paths:
            print(f"\n{'─'*50}")
            process_single_file(fp, config, exclude_channels, ref_channel, args.output)
        print("\nDone.")
        return
    
    # ---- INTERACTIVE MODE ----
    print("\n" + "="*50)
    print("  XAS Bad Channel Filter — Interactive Mode")
    print("="*50)
    print(f"\n  Current settings:")
    print(f"    Outlier threshold:     {config.outlier_threshold}")
    print(f"    Iterations:            {config.iterations}")
    print(f"    Correlation threshold: {config.correlation_threshold}")
    print(f"    Window size fraction:  {config.window_size_fraction}")
    print(f"    Reference foil:        {ref_channel}")
    if exclude_channels:
        print(f"    Excluded channels:     {exclude_channels}")
    
    if prompt_yes_no("\nAdjust settings before starting?", default=False):
        config = interactive_config(config)
        exclude_channels = interactive_exclude_channels()
        ref_channel = interactive_ref_channel()
    
    # Process first file with retry loop
    first_file = file_paths[0]
    while True:
        result = process_single_file(first_file, config, exclude_channels, 
                                      ref_channel, args.output)
        
        if result is None:
            if not prompt_yes_no("\nRetry with different settings?"):
                return
            config = interactive_config(config)
            exclude_channels = interactive_exclude_channels()
            continue
        
        # Ask user what to do
        if len(file_paths) > 1:
            print(f"\n  {len(file_paths) - 1} files remaining.")
            print("  Options:")
            print("    c = continue processing remaining files with these settings")
            print("    a = adjust settings and re-run this file")
            print("    q = quit")
            
            choice = input("\n  Choice [c]: ").strip().lower()
            if not choice or choice == 'c':
                break
            elif choice == 'a':
                config = interactive_config(config)
                new_exclude = interactive_exclude_channels()
                if new_exclude is not None:
                    exclude_channels = new_exclude
                continue
            else:
                print("Exiting.")
                return
        else:
            # Single file, we're done
            break
    
    # Process remaining files
    for fp in file_paths[1:]:
        print(f"\n{'─'*50}")
        process_single_file(fp, config, exclude_channels, ref_channel, args.output)
    
    print(f"\nDone. Processed {len(file_paths)} files.")

if __name__ == "__main__":
    main()