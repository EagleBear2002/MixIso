#!/usr/bin/env python3
"""
Fast WSL allocation script using Python
Allocates isolation levels to benchmark workload files in parallel
"""

import os
import sys
import json
import subprocess
import csv
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

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

def allocate_file(input_file, output_file, classpath, debug=False):
    """
    Allocate a single workload file
    Returns: (filename, success, error_message)
    """
    filename = Path(input_file).name
    
    try:
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
            timeout=30
        )
        
        if result.returncode == 0:
            return (filename, True, None)
        else:
            error_msg = result.stderr
            if debug:
                print(f"Debug - Error output:\n{error_msg}")
            return (filename, False, error_msg[:200])
    except Exception as e:
        return (filename, False, str(e))

def create_allocation_plots(allocated_dir):
    """
    Create visualization plots for the isolation level allocation results
    Matching the style of Figure 8 in the user's reference.
    """
    print(f"\n{CYAN}Creating visualization plots for benchmarks...{NC}")
    
    # Mapping from JSON values to labels/colors
    # SER: Red, SI: Orange, PSI: Yellow, PC: Cyan, RA: Green
    level_map = {
        'SERIALIZABLE': 'SER',
        'SNAPSHOT_ISOLATION': 'SI',
        'PARALLEL_SNAPSHOT_ISOLATION': 'PSI',
        'PREFIX_CONSISTENCY': 'PC',
        'CAUSAL_CONSISTENCY': 'CC',
        'READ_ATOMIC': 'RA'
    }
    
    # Preferred order and colors from reference image
    levels = ['SER', 'SI', 'PSI', 'PC', 'RA']
    colors = ['red', 'orange', 'yellow', 'cyan', 'lime']
    
    # Data structure: benchmark -> instances -> level distributions
    all_data = {}
    
    # Read all allocated files
    json_files = list(allocated_dir.glob('*.json'))
    if not json_files:
        print(f"{YELLOW}No allocated JSON files found in {allocated_dir}{NC}")
        return
    
    print(f"Analyzing {len(json_files)} files for plotting...")
    
    for json_file in json_files:
        filename = json_file.name
        # Filename example: SmallBank_100t_500k_1.json
        parts = filename.replace('.json', '').split('_')
        if len(parts) < 1: continue
        
        bench_type = parts[0] # SmallBank, Courseware, or TPCC (case sensitive matching depends on filename)
        
        # Try to extract the instance number (the last part)
        try:
            instance_num = int(parts[-1])
        except (ValueError, IndexError):
            # Fallback if the last part is not a number
            instance_num = filename
            
        if bench_type not in all_data:
            all_data[bench_type] = {}
        
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                templates = data.get('templates', [])
                total = len(templates)
                
                counts = {lvl: 0 for lvl in levels}
                # Track CC separately if it exists but map it to RA for the 5-level chart if needed
                # or just ignore it if it doesn't appear.
                for t in templates:
                    lvl_raw = t.get('isolationLevel')
                    lvl_mapped = level_map.get(lvl_raw, 'RA')
                    if lvl_mapped == 'CC': lvl_mapped = 'RA' # Map CC to RA for simplicity in the 5-color chart
                    
                    if lvl_mapped in counts:
                        counts[lvl_mapped] += 1
                
                # Convert to percentages
                if total > 0:
                    percentages = {lvl: (counts[lvl] / total * 100) for lvl in levels}
                else:
                    percentages = {lvl: 0 for lvl in levels}
                    
                all_data[bench_type][instance_num] = percentages
                
        except Exception as e:
            print(f"{YELLOW}  Warning: Error reading {filename}: {e}{NC}")
    
    # Define benchmarks and ensure they are sorted/selected correctly
    # Filenames use 'TPCC', 'SmallBank', 'Courseware'
    benchmarks = ['SmallBank', 'Courseware', 'TPCC']
    # Filter to only those we actually have data for
    benchmarks = [b for b in benchmarks if b in all_data]
    
    if not benchmarks:
        print(f"{YELLOW}No recognized benchmark data found. Available keys: {list(all_data.keys())}{NC}")
        return

    # Start plotting - 1 row, N columns
    num_plots = len(benchmarks)
    fig, axes = plt.subplots(1, num_plots, figsize=(6 * num_plots, 5), sharey=True)
    if num_plots == 1:
        axes = [axes]
    
    for i, bench in enumerate(benchmarks):
        ax = axes[i]
        bench_results = all_data[bench]
        
        # Sort by instance number if they are numeric
        try:
            sorted_instances = sorted(bench_results.keys(), key=lambda x: int(x) if isinstance(x, int) or (isinstance(x, str) and x.isdigit()) else 0)
        except:
            sorted_instances = sorted(bench_results.keys())
            
        x = np.arange(len(sorted_instances))
        bottom = np.zeros(len(sorted_instances))
        
        for lvl_idx, lvl in enumerate(levels):
            y = [bench_results[inst][lvl] for inst in sorted_instances]
            ax.bar(x, y, bottom=bottom, label=lvl, color=colors[lvl_idx], width=0.8, edgecolor='none')
            bottom += y
            
        ax.set_title(bench, y=-0.2, fontsize=14, fontweight='bold')
        
        # Set x-ticks. If we have many, only show some.
        if len(x) > 10:
            step = max(1, len(x) // 10)
            ax.set_xticks(x[::step])
            ax.set_xticklabels([sorted_instances[j] for j in range(0, len(x), step)])
        else:
            ax.set_xticks(x)
            ax.set_xticklabels(sorted_instances)
            
        if i == 0:
            ax.set_ylabel('Percentage (%)', fontsize=12)
            
        ax.set_ylim(0, 100)
        ax.grid(axis='y', linestyle='--', alpha=0.3)
        # Remove top/right spines for cleaner look
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)

    # Add legend at the top
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc='upper center', bbox_to_anchor=(0.5, 0.98),
               ncol=5, fontsize=12, frameon=False)
    
    plt.tight_layout(rect=[0, 0.05, 1, 0.92])
    
    # Save the plot to the data directory
    output_file = Path('data') / 'bench_allocation_distribution.png'
    try:
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"{GREEN}✓ Allocation distribution plot saved to {output_file}{NC}")
    except Exception as e:
        print(f"{RED}Error saving plot: {e}{NC}")
    finally:
        plt.close()
    
    # Generate CSV file with distribution data
    csv_file = Path('data') / 'bench_allocation_distribution.csv'
    try:
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # Write header
            writer.writerow(['Benchmark', 'Instance', 'SER', 'SI', 'PSI', 'PC', 'RA'])
            
            # Write data for each benchmark and instance
            for bench in benchmarks:
                bench_results = all_data[bench]
                sorted_instances = sorted(bench_results.keys(), key=lambda x: int(x) if isinstance(x, int) or (isinstance(x, str) and x.isdigit()) else 0)
                
                for inst in sorted_instances:
                    dist = bench_results[inst]
                    writer.writerow([
                        bench,
                        inst,
                        f"{dist['SER']:.2f}%",
                        f"{dist['SI']:.2f}%",
                        f"{dist['PSI']:.2f}%",
                        f"{dist['PC']:.2f}%",
                        f"{dist['RA']:.2f}%"
                    ])
        
        print(f"{GREEN}✓ Distribution data saved to {csv_file}{NC}")
    except Exception as e:
        print(f"{RED}Error saving CSV: {e}{NC}")

def main():
    project_dir = get_project_dir()
    os.chdir(project_dir)
    
    bench_workload_dir = project_dir / 'data' / 'bench_workload'
    allocated_dir = project_dir / 'data' / 'allocated_bench_workload'
    build_dir = project_dir / 'target'
    classes_dir = build_dir / 'classes'
    
    print("=" * 42)
    print("Benchmark Workload Allocation (Fast Mode)")
    print("=" * 42)
    print()
    
    # Check prerequisites
    if not bench_workload_dir.exists():
        print(f"{RED}Error: Benchmark workload directory not found{NC}")
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
    workload_files = sorted(bench_workload_dir.glob('*.json'))
    total_files = len(workload_files)
    
    if total_files == 0:
        print(f"{RED}No JSON files found in {bench_workload_dir}{NC}")
        return 1
    
    print(f"Found {total_files} benchmark workload files to allocate")
    print(f"Starting allocation at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Process files in parallel
    successful = 0
    failed = 0
    failed_files = []
    
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
            filename, success, error = future.result()
            
            if success:
                print(f"[{i:2d}/{total_files}] {filename:<30} {GREEN}✓{NC}")
                successful += 1
            else:
                print(f"[{i:2d}/{total_files}] {filename:<30} {RED}✗{NC}")
                failed += 1
                failed_files.append(filename)
    
    print()
    print("=" * 42)
    print("Allocation Summary")
    print("=" * 42)
    print(f"Total files processed: {total_files}")
    print(f"Successful: {GREEN}{successful}{NC}")
    
    if failed > 0:
        print(f"Failed: {RED}{failed}{NC}")
        print()
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
        
        # Create plots
        create_allocation_plots(allocated_dir)
        
        print(f"Completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        return 0

if __name__ == '__main__':
    sys.exit(main())
