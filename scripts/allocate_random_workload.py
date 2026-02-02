#!/usr/bin/env python3
"""
Allocate isolation levels to random workload files in parallel
Records execution time for each file and outputs performance metrics to CSV
"""

import os
import sys
import json
import csv
import subprocess
import time
import re
import random
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Tuple, List, Dict, Optional
import matplotlib.pyplot as plt
import numpy as np
try:
    import pandas as pd
except ImportError:
    pd = None

# Set UTF-8 encoding for Windows compatibility
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

# Colors
GREEN = '\033[0;32m'
RED = '\033[0;31m'
YELLOW = '\033[1;33m'
CYAN = '\033[0;36m'
NC = '\033[0m'  # No Color

def get_project_dir():
    """Get the project root directory"""
    script_dir = Path(__file__).parent.absolute()
    project_dir = script_dir.parent
    return project_dir

def get_classpath(classes_dir, project_dir):
    """
    Construct the Java classpath with all dependencies.
    """
    classpath_parts = [str(classes_dir)]
    
    # Add target/dependency directory if it exists
    dependency_dir = project_dir / 'target' / 'dependency'
    if dependency_dir.exists():
        if sys.platform == 'win32':
            classpath_parts.append(str(dependency_dir / '*'))
        else:
            # On non-Windows, we might need to expand the wildcard or use it as is
            # Java handles the * wildcard in classpath
            classpath_parts.append(str(dependency_dir / '*'))
            
    # Also try to find dependencies in Maven local repository as fallback
    m2_repo = Path.home() / '.m2' / 'repository'
    if m2_repo.exists():
        required_deps = [
            'com/google/code/gson/gson/2.8.9/gson-2.8.9.jar',
            'com/fasterxml/jackson/core/jackson-databind/2.13.3/jackson-databind-2.13.3.jar',
            'com/fasterxml/jackson/core/jackson-core/2.13.3/jackson-core-2.13.3.jar',
            'com/fasterxml/jackson/core/jackson-annotations/2.13.3/jackson-annotations-2.13.3.jar'
        ]
        for dep in required_deps:
            dep_path = m2_repo / dep
            if dep_path.exists():
                classpath_parts.append(str(dep_path))
    
    return classpath_parts

def allocate_file(input_file, output_file, classpath, debug=False) -> Tuple[str, bool, Optional[str], float]:
    """
    Allocate a single workload file and record execution time
    Returns: (filename, success, error_message, execution_time_seconds)
    """
    filename = Path(input_file).name
    
    try:
        start_time = time.time()
        
        # Use system 'java' by default, or JAVA_HOME if set
        java_cmd = 'java'
        java_home = os.environ.get('JAVA_HOME')
        if java_home:
            candidate = Path(java_home) / 'bin' / ('java.exe' if sys.platform == 'win32' else 'java')
            if candidate.exists():
                java_cmd = str(candidate)
        
        cmd = [
            java_cmd,
            '-cp', classpath,
            'algorithm.Allocator',
            'allocate',
            str(input_file),
            str(output_file)
        ]
        
        if debug:
            print(f"\nDebug - Command: {' '.join(cmd)}")
            print(f"Debug - Classpath length: {len(classpath)}")
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            encoding='utf-8',
            timeout=300
        )
        
        execution_time = time.time() - start_time
        
        if result.returncode == 0:
            return (filename, True, None, execution_time)
        else:
            error_msg = result.stderr.decode('utf-8', errors='ignore')
            if debug:
                print(f"Debug - Error output:\n{error_msg}")
            return (filename, False, error_msg[:200], execution_time)
    except subprocess.TimeoutExpired:
        execution_time = time.time() - start_time
        return (filename, False, "Timeout (>300s)", execution_time)
    except Exception as e:
        execution_time = time.time() - start_time
        return (filename, False, str(e), execution_time)

def create_plots_from_analysis_csv(analysis_csv):
    """
    Create unified visualization with 4 subplots from the analysis CSV file.
    Shows mean execution time with error bars for each varying parameter.
    """
    if not analysis_csv.exists():
        print(f"{YELLOW}Warning: Analysis CSV not found, skipping plot generation{NC}")
        return
    
    try:
        if pd is None:
            print(f"{YELLOW}Warning: pandas not available, skipping plot generation{NC}")
            return
            
        # Read analysis CSV
        df = pd.read_csv(analysis_csv)
        
        # Create a unified figure with 3 subplots in a row
        fig, axes = plt.subplots(1, 3, figsize=(20, 6))
        fig.suptitle('Allocation Performance vs Workload Parameters', fontsize=16, fontweight='bold')
        
        # Mapping of plot names to subplot positions
        plot_positions = {
            'txn_count_vs_time': axes[0],
            'op_per_txn_vs_time': axes[1],
            'max_key_vs_time': axes[2]
        }
        
        # Formatting info
        param_labels = {
            'txns': 'Number of Transactions',
            'max_ops': 'Max Operations per Transaction',
            'max_key': 'Max Key ID (in thousands)'
        }
        
        # Plot data for each parameter
        for plot_name, ax in plot_positions.items():
            plot_df = df[df['plot'] == plot_name]
            
            if plot_df.empty:
                ax.text(0.5, 0.5, 'No data', ha='center', va='center', transform=ax.transAxes)
                ax.set_title(plot_name)
                continue
            
            # Extract data
            x_values = plot_df['vary_value'].values
            y_means = plot_df['mean'].values
            y_stds = plot_df['std'].values
            
            # Get varying parameter name
            vary_param = plot_df['vary_variable'].iloc[0]
            
            # Get base config from first row
            base_config_items = []
            if pd.notna(plot_df['txn_count'].iloc[0]) and plot_df['txn_count'].iloc[0] != '':
                base_config_items.append(f"txn={int(plot_df['txn_count'].iloc[0])}")
            if pd.notna(plot_df['op_per_txn'].iloc[0]) and plot_df['op_per_txn'].iloc[0] != '':
                base_config_items.append(f"ops={int(plot_df['op_per_txn'].iloc[0])}")
            if pd.notna(plot_df['max_key'].iloc[0]) and plot_df['max_key'].iloc[0] != '':
                base_config_items.append(f"key={int(plot_df['max_key'].iloc[0])}")
            if pd.notna(plot_df['read_only_percent'].iloc[0]) and plot_df['read_only_percent'].iloc[0] != '':
                base_config_items.append(f"ro={int(plot_df['read_only_percent'].iloc[0])}%")
            
            base_config_str = ', '.join(base_config_items)
            
            # Plot line with error bars
            ax.errorbar(x_values, y_means, yerr=y_stds, marker='o', linewidth=2.5,
                       markersize=8, capsize=5, color='steelblue', ecolor='steelblue',
                       label='Mean Execution Time')
            
            # Formatting
            ax.set_xlabel(param_labels.get(vary_param, vary_param), fontsize=11, fontweight='bold')
            ax.set_ylabel('Execution Time (seconds)', fontsize=11, fontweight='bold')
            ax.set_title(f'{plot_name} (Base: {base_config_str})', fontsize=12, fontweight='bold')
            ax.grid(True, alpha=0.3, linestyle='--')
            ax.legend(loc='best', fontsize=10)
        
        plt.tight_layout()
        
        # Save unified figure
        output_file = analysis_csv.parent / 'allocation_performance_unified.png'
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"{GREEN}✓ Unified plot saved to {output_file}{NC}")
        plt.close()
        
    except Exception as e:
        print(f"{RED}Error creating plots: {e}{NC}")
        plt.close()

def generate_analysis_csv(result_csv, analysis_csv):
    """
    Generate analysis CSV with statistics grouped by varying parameters.
    Format: plot, vary_variable, vary_value, txn_count, op_per_txn, max_key, read_only_percent, mean, std, sample_count
    """
    if not result_csv.exists():
        print(f"{YELLOW}Warning: Performance CSV not found{NC}")
        return False
    
    # Parse raw performance data
    all_data = []
    try:
        with open(result_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['status'] != 'success':
                    continue
                
                filename = row['filename']
                execution_time = float(row['execution_time_seconds'])
                
                # Parse filename: workload_{txns}t_{max_ops}o_{max_key}k_{read_only}r_{case_num}.json
                match = re.match(
                    r'workload_(\d+)t_(\d+)o_(\d+)k_(\d+)r_(\d+)\.json',
                    filename
                )
                
                if match:
                    txns, max_ops, max_key, read_only, case_num = map(int, match.groups())
                    all_data.append({
                        'txns': txns,
                        'max_ops': max_ops,
                        'max_key': max_key,
                        'read_only': read_only,
                        'case_num': case_num,
                        'execution_time': execution_time
                    })
    except Exception as e:
        print(f"{RED}Error reading performance CSV: {e}{NC}")
        return False
    
    if not all_data:
        print(f"{YELLOW}Warning: No valid data found{NC}")
        return False
    
    # Generate analysis data
    from collections import Counter, defaultdict
    
    analysis_data = []
    
    # Define varying parameters and their plot names
    varying_params = {
        'txns': 'txn_count_vs_time',
        'max_ops': 'op_per_txn_vs_time',
        'max_key': 'max_key_vs_time',
        'read_only': 'read_only_vs_time'
    }
    
    for varying_param, plot_name in varying_params.items():
        # Get other parameters
        other_params = {'txns', 'max_ops', 'max_key', 'read_only'} - {varying_param}
        
        # Find most common values for other parameters
        param_counts = {}
        for param in other_params:
            param_counts[param] = Counter(item[param] for item in all_data)
        
        base_config = {param: param_counts[param].most_common(1)[0][0] 
                      for param in other_params}
        
        # Filter data matching base configuration
        filtered_data = [item for item in all_data 
                        if all(item[param] == base_config[param] for param in other_params)]
        
        # Group by varying parameter value
        grouped = defaultdict(list)
        for item in filtered_data:
            param_value = item[varying_param]
            grouped[param_value].append(item['execution_time'])
        
        # Create analysis rows
        for param_value in sorted(grouped.keys()):
            times = grouped[param_value]
            mean_time = np.mean(times)
            std_time = np.std(times)
            sample_count = len(times)
            
            row = {
                'plot': plot_name,
                'vary_variable': varying_param,
                'vary_value': float(param_value),
                'txn_count': float(base_config['txns']) if 'txns' in base_config else '',
                'op_per_txn': float(base_config['max_ops']) if 'max_ops' in base_config else '',
                'max_key': float(base_config['max_key']) if 'max_key' in base_config else '',
                'read_only_percent': float(base_config['read_only']) if 'read_only' in base_config else '',
                'mean': mean_time,
                'std': std_time,
                'sample_count': sample_count
            }
            analysis_data.append(row)
    
    # Write to CSV
    try:
        with open(analysis_csv, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['plot', 'vary_variable', 'vary_value', 'txn_count', 'op_per_txn', 
                         'max_key', 'read_only_percent', 'mean', 'std', 'sample_count']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(analysis_data)
        print(f"{GREEN}✓ Analysis CSV written to {analysis_csv}{NC}")
        return True
    except Exception as e:
        print(f"{RED}Error writing analysis CSV: {e}{NC}")
        return False

def parse_csv_and_create_plots(result_csv):
    """
    Parse the CSV file and create performance visualization plots.
    Analyzes single variables by grouping data with identical other parameters.
    """
    if not result_csv.exists():
        print(f"{YELLOW}Warning: CSV file not found, skipping plot generation{NC}")
        return
    
    # List to store all parsed data
    all_data = []
    
    try:
        with open(result_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Only consider successful executions
                if row['status'] != 'success':
                    continue
                
                filename = row['filename']
                execution_time = float(row['execution_time_seconds'])
                
                # Parse filename: workload_{txns}t_{max_ops}o_{max_key}k_{read_only}r_{case_num}.json
                match = re.match(
                    r'workload_(\d+)t_(\d+)o_(\d+)k_(\d+)r_(\d+)\.json',
                    filename
                )
                
                if match:
                    txns, max_ops, max_key, read_only, case_num = map(int, match.groups())
                    all_data.append({
                        'txns': txns,
                        'max_ops': max_ops,
                        'max_key': max_key,
                        'read_only': read_only,
                        'case_num': case_num,
                        'execution_time': execution_time
                    })
        
        if not all_data:
            print(f"{YELLOW}Warning: No valid data found in CSV{NC}")
            return
        
        # Create figure with 3 subplots in a row
        fig, axes = plt.subplots(1, 3, figsize=(18, 6))
        fig.suptitle('Allocation Performance vs Workload Parameters', fontsize=16, fontweight='bold')
        
        # Helper function to analyze single variable
        def get_single_variable_data(data, varying_param):
            """Group data by varying parameter, keeping other parameters constant"""
            grouped = {}
            
            for item in data:
                key = (
                    item['txns'],
                    item['max_ops'],
                    item['max_key']
                )
                
                if key not in grouped:
                    grouped[key] = {}
                
                param_value = item[varying_param]
                if param_value not in grouped[key]:
                    grouped[key][param_value] = []
                
                grouped[key][param_value].append(item['execution_time'])
            
            # For each configuration of other parameters, collect the times for this parameter
            result = {}
            for config, param_times in grouped.items():
                # Only use configurations where other params are fixed (have data for all cases of varying_param)
                for param_value, times in param_times.items():
                    if param_value not in result:
                        result[param_value] = []
                    result[param_value].extend(times)
            
            return result
        
        # Better approach: find base configuration and vary one parameter
        def get_varied_data(data, varying_param):
            """Extract data varying one parameter while others are fixed"""
            # Find the most common values for other parameters (representing base config)
            from collections import Counter
            
            other_params = {'txns', 'max_ops', 'max_key'} - {varying_param}
            
            param_counts = {}
            for param in other_params:
                param_counts[param] = Counter(item[param] for item in data)
            
            # Get most common value for each other parameter
            base_config = {param: param_counts[param].most_common(1)[0][0] 
                          for param in other_params}
            
            # Filter data matching base configuration
            filtered_data = [item for item in data 
                           if all(item[param] == base_config[param] for param in other_params)]
            
            # Group by varying parameter
            result = {}
            for item in filtered_data:
                param_value = item[varying_param]
                if param_value not in result:
                    result[param_value] = []
                result[param_value].append(item['execution_time'])
            
            return result, base_config
        
        # Helper function to plot a parameter with line chart
        def plot_parameter(ax, param_name, data, xlabel):
            varied_data, base_config = get_varied_data(data, param_name)
            
            if not varied_data:
                ax.text(0.5, 0.5, 'No data', ha='center', va='center', transform=ax.transAxes)
                ax.set_title(param_name)
                return
            
            # Sort by parameter value
            sorted_items = sorted(varied_data.items(), key=lambda x: x[0])
            x_values = [x[0] for x in sorted_items]
            y_means = [np.mean(y) for _, y in sorted_items]
            y_stds = [np.std(y) if len(y) > 1 else 0 for _, y in sorted_items]
            
            # Plot line chart with error bars
            ax.errorbar(x_values, y_means, yerr=y_stds, marker='o', linewidth=2, 
                       markersize=8, capsize=5, color='steelblue', ecolor='steelblue',
                       label='Execution time')
            
            ax.set_xlabel(xlabel)
            ax.set_ylabel('Execution Time (seconds)')
            ax.set_title(f'Performance vs {param_name}')
            ax.grid(True, alpha=0.3)
            
            # Add base configuration info to title
            other_params = {'txns', 'max_ops', 'max_key', 'read_only'} - {param_name}
            config_str = ', '.join([f"{p}={base_config[p]}" for p in sorted(other_params)])
            ax.set_title(f'Performance vs {param_name}\n(Base config: {config_str})', fontsize=11)
        
        # Plot each parameter
        plot_parameter(axes[0], 'txns', all_data, 'Number of Transactions')
        plot_parameter(axes[1], 'max_ops', all_data, 'Max Operations per Txn')
        plot_parameter(axes[2], 'max_key', all_data, 'Max Key ID (in thousands)')
        
        plt.tight_layout()
        
        # Save the plot
        output_file = result_csv.parent / 'allocation_performance.png'
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"{GREEN}✓ Performance plot saved to {output_file}{NC}")
        plt.close()
        
    except ImportError:
        print(f"{YELLOW}Warning: matplotlib not available, skipping plot generation{NC}")
    except Exception as e:
        print(f"{RED}Error creating plots: {e}{NC}")

def main():
    project_dir = get_project_dir()
    os.chdir(project_dir)
    
    random_workload_dir = project_dir / 'data' / 'random_workload'
    allocated_dir = project_dir / 'data' / 'allocated_random_workload'
    result_csv = project_dir / 'data' / 'allocation_performance.csv'
    build_dir = project_dir / 'target'
    classes_dir = build_dir / 'classes'
    
    print("=" * 50)
    print("Random Workload Allocation (Fast Mode)")
    print("=" * 50)
    print()
    
    # Check prerequisites
    if not random_workload_dir.exists():
        print(f"{RED}Error: Random workload directory not found{NC}")
        return 1
    
    # Recompile Java code
    print(f"{CYAN}Recompiling Java code...{NC}")
    maven_available = False
    try:
        result = subprocess.run(
            ['mvn', 'clean', 'compile', 'dependency:copy-dependencies'],
            capture_output=True,
            encoding='utf-8',
            timeout=300
        )
        if result.returncode == 0:
            maven_available = True
            print(f"{GREEN}✓ Java code compiled successfully{NC}")
        else:
            print(f"{YELLOW}Warning: Maven compilation failed{NC}")
            print(f"{YELLOW}Checking for existing compiled classes...{NC}")
    except Exception as e:
        print(f"{YELLOW}Warning: Failed to run Maven: {e}{NC}")
        print(f"{YELLOW}Checking for existing compiled classes...{NC}")
    
    print()
    
    # Check if we have compiled classes
    if not classes_dir.exists():
        if not maven_available:
            print(f"{RED}Error: Build output directory not found and Maven compilation failed.{NC}")
            print(f"{RED}Please manually run: mvn clean compile{NC}")
            return 1
        else:
            print(f"{RED}Error: Build output directory not found{NC}")
            return 1
    
    print(f"{GREEN}Found compiled classes at {classes_dir}{NC}")
    print()
    
    # Create output directory
    allocated_dir.mkdir(parents=True, exist_ok=True)
    
    # Build classpath
    classpath_parts = get_classpath(classes_dir, project_dir)
    if sys.platform.startswith('win'):
        classpath = ';'.join(classpath_parts)
    else:
        classpath = ':'.join(classpath_parts)
    
    # Find all workload files
    workload_files = sorted(random_workload_dir.glob('*.json'))
    total_files = len(workload_files)
    
    if total_files == 0:
        print(f"{RED}No JSON files found in {random_workload_dir}{NC}")
        return 1
    
    # Shuffle workload files for random execution order
    random.shuffle(workload_files)
    
    print(f"Found {total_files} random workload files to allocate")
    print(f"Starting allocation at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Execution order: randomized{NC}")
    print()
    
    # Track performance data
    performance_data: List[Dict] = []
    
    # Process files in parallel
    successful = 0
    failed = 0
    failed_files = []
    total_time = 0
    
    max_workers = 4  # Limit concurrent allocations
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        
        for input_file in workload_files:
            output_file = allocated_dir / input_file.name
            future = executor.submit(
                allocate_file,
                input_file,
                output_file,
                classpath
            )
            futures[future] = input_file.name
        
        for i, future in enumerate(as_completed(futures), 1):
            filename, success, error, execution_time = future.result()
            total_time += execution_time
            
            # Record performance data
            performance_data.append({
                'filename': filename,
                'status': 'success' if success else 'failed',
                'execution_time_seconds': f'{execution_time:.2f}',
                'error_message': error if error else ''
            })
            
            if success:
                print(f"[{i:2d}/{total_files}] {filename:<40} {GREEN}✓ {execution_time:6.2f}s{NC}")
                successful += 1
            else:
                print(f"[{i:2d}/{total_files}] {filename:<40} {RED}✗ {execution_time:6.2f}s{NC}")
                failed += 1
                failed_files.append(filename)
    
    # Write performance data to CSV (intermediate file)
    csv_dir = result_csv.parent
    csv_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        with open(result_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(
                f,
                fieldnames=['filename', 'status', 'execution_time_seconds', 'error_message']
            )
            writer.writeheader()
            writer.writerows(performance_data)
    except Exception as e:
        print(f"\n{RED}✗ Failed to write CSV: {e}{NC}")
        return 1
    
    # Generate analysis CSV with statistics
    print()
    analysis_csv = csv_dir / 'allocation_performance_analysis.csv'
    generate_analysis_csv(result_csv, analysis_csv)
    
    # Create unified visualization plot from analysis CSV
    print()
    create_plots_from_analysis_csv(analysis_csv)
    
    print()
    print("=" * 50)
    print("Allocation Summary")
    print("=" * 50)
    print(f"Total files processed: {total_files}")
    print(f"Successful: {GREEN}{successful}{NC}")
    print(f"Failed: {RED if failed > 0 else GREEN}{failed}{NC}")
    print(f"Total execution time: {total_time:.2f}s")
    print(f"Average time per file: {total_time/total_files:.2f}s")
    print()
    
    if failed > 0:
        print("Failed files:")
        for f in failed_files:
            print(f"  - {f}")
        return 1
    else:
        print(f"{GREEN}All allocations completed successfully!{NC}")
        print()
        allocated_count = len(list(allocated_dir.glob('*.json')))
        print(f"Output directory: {allocated_dir}")
        print(f"{allocated_count} files allocated")
        print()
        print("Generated files:")
        analysis_csv = result_csv.parent / 'allocation_performance_analysis.csv'
        print(f"  {analysis_csv}")
        unified_plot = result_csv.parent / 'allocation_performance_unified.png'
        print(f"  {unified_plot}")
        print()
        print(f"Completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        return 0

if __name__ == '__main__':
    sys.exit(main())
