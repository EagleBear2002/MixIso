#!/usr/bin/env python3
"""
Comprehensive performance experiment exploring the relationship between 
workload parameters (txns, max-ops, max-key, read-only) and allocation performance.

Uses control variable method: each sub-experiment varies only one parameter while 
keeping others constant.
"""

import os
import sys
import csv
import subprocess
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple
import matplotlib.pyplot as plt
import numpy as np

# Set UTF-8 encoding for Windows compatibility
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

# Colors
GREEN = '\033[0;32m'
RED = '\033[0;31m'
YELLOW = '\033[1;33m'
CYAN = '\033[0;36m'
NC = '\033[0m'

def get_project_dir() -> Path:
    """Get the project root directory"""
    script_dir = Path(__file__).parent.absolute()
    project_dir = script_dir.parent
    return project_dir

def run_command(cmd: List[str], description: str = "") -> bool:
    """
    Run a command and report results.
    
    Args:
        cmd: Command to run as list of strings
        description: Description of what the command does
        
    Returns:
        True if successful, False otherwise
    """
    if description:
        print(f"{CYAN}{description}{NC}")
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', timeout=600)
        if result.returncode != 0:
            print(f"{RED}✗ Command failed: {' '.join(cmd)}{NC}")
            if result.stderr:
                print(f"  Error: {result.stderr[:200]}")
            return False
        return True
    except subprocess.TimeoutExpired:
        print(f"{RED}✗ Command timeout{NC}")
        return False
    except Exception as e:
        print(f"{RED}✗ Exception: {e}{NC}")
        return False

def generate_workload(txns: int, max_ops: int, max_key: int, read_only: int, cases: int) -> bool:
    """
    Generate random workload with specified parameters.
    
    Args:
        txns: Total number of transactions
        max_ops: Maximum operations per transaction
        max_key: Maximum key ID
        read_only: Percentage of read-only transactions
        cases: Number of workload cases to generate
        
    Returns:
        True if successful, False otherwise
    """
    project_dir = get_project_dir()
    script_path = project_dir / 'scripts' / 'generate_random_workload.py'
    
    cmd = [
        sys.executable,
        str(script_path),
        '--txns', str(txns),
        '--max-ops', str(max_ops),
        '--max-key', str(max_key),
        '--read-only', str(read_only),
        '--cases', str(cases)
    ]
    
    return run_command(cmd, f"  Generating workload: txns={txns}, max-ops={max_ops}, max-key={max_key}, read-only={read_only}%")

def run_allocation() -> bool:
    """
    Run allocation on all generated workload files.
    
    Returns:
        True if successful, False otherwise
    """
    project_dir = get_project_dir()
    script_path = project_dir / 'scripts' / 'allocate_random_workload.py'
    
    cmd = [sys.executable, str(script_path)]
    
    return run_command(cmd, "  Running allocation...")

def clean_random_workload_dir() -> None:
    """Clean the random workload directory"""
    project_dir = get_project_dir()
    workload_dir = project_dir / 'data' / 'random_workload'
    
    if workload_dir.exists():
        for file in workload_dir.glob('*.json'):
            file.unlink()

def parse_csv_by_params(csv_file: Path) -> Dict[str, Dict[str, List[float]]]:
    """
    Parse the allocation_performance.csv and group by parameters.
    
    Returns a dict mapping parameter combinations to execution times.
    Format: {
        'txns': {param_value: [times...]},
        'max-ops': {param_value: [times...]},
        'max-key': {param_value: [times...]},
        'read-only': {param_value: [times...]}
    }
    """
    result = {
        'txns': {},
        'max-ops': {},
        'max-key': {},
        'read-only': {}
    }
    
    if not csv_file.exists():
        return result
    
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['status'] != 'success':
                    continue
                
                filename = row['filename']
                execution_time = float(row['execution_time_seconds'])
                
                # Parse filename: workload_{txns}t_{max_ops}o_{max_key}k_{read_only}r_{case_num}.json
                # Example: workload_500t_10o_1k_50r_1.json
                match = re.match(
                    r'workload_(\d+)t_(\d+)o_(\d+)k_(\d+)r_\d+\.json',
                    filename
                )
                
                if match:
                    txns, max_ops, max_key, read_only = map(int, match.groups())
                    
                    # Group by parameter
                    if txns not in result['txns']:
                        result['txns'][txns] = []
                    result['txns'][txns].append(execution_time)
                    
                    if max_ops not in result['max-ops']:
                        result['max-ops'][max_ops] = []
                    result['max-ops'][max_ops].append(execution_time)
                    
                    if max_key not in result['max-key']:
                        result['max-key'][max_key] = []
                    result['max-key'][max_key].append(execution_time)
                    
                    if read_only not in result['read-only']:
                        result['read-only'][read_only] = []
                    result['read-only'][read_only].append(execution_time)
    
    except Exception as e:
        print(f"{RED}Error parsing CSV: {e}{NC}")
    
    return result

def create_plots(csv_file: Path) -> None:
    """Create visualization plots for the experiment results"""
    
    print(f"\n{CYAN}Creating visualization plots...{NC}")
    
    data = parse_csv_by_params(csv_file)
    
    # Create a figure with 4 subplots
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle('Allocation Performance vs Workload Parameters', fontsize=16, fontweight='bold')
    
    # Helper function to plot a parameter
    def plot_parameter(ax, param_name, param_data, param_label, unit=''):
        if not param_data:
            ax.text(0.5, 0.5, 'No data', ha='center', va='center')
            ax.set_title(param_name)
            return
        
        # Sort by parameter value
        sorted_items = sorted(param_data.items(), key=lambda x: x[0])
        x_values = [str(x[0]) for x in sorted_items]
        y_means = [np.mean(y) for _, y in sorted_items]
        y_stds = [np.std(y) if len(y) > 1 else 0 for _, y in sorted_items]
        
        # Plot with error bars
        x_pos = np.arange(len(x_values))
        ax.bar(x_pos, y_means, yerr=y_stds, capsize=5, color='steelblue', alpha=0.7)
        ax.set_xlabel(param_label)
        ax.set_ylabel('Execution Time (seconds)')
        ax.set_title(f'Performance vs {param_name}')
        ax.set_xticks(x_pos)
        ax.set_xticklabels(x_values, rotation=45, ha='right')
        ax.grid(axis='y', alpha=0.3)
    
    # Plot each parameter
    plot_parameter(axes[0, 0], 'txns', data['txns'], 'Number of Transactions')
    plot_parameter(axes[0, 1], 'max-ops', data['max-ops'], 'Max Operations per Txn')
    plot_parameter(axes[1, 0], 'max-key', data['max-key'], 'Max Key ID')
    plot_parameter(axes[1, 1], 'read-only', data['read-only'], 'Read-only %')
    
    plt.tight_layout()
    
    # Save the plot
    project_dir = get_project_dir()
    output_file = project_dir / 'data' / 'allocation_performance.png'
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"{GREEN}✓ Plot saved to {output_file}{NC}")
    except Exception as e:
        print(f"{RED}Error saving plot: {e}{NC}")
    finally:
        plt.close()

def run_experiment(name: str, base_params: Dict, varying_param: str, param_values: List[int], cases: int = 5) -> bool:
    """
    Run a single sub-experiment with one varying parameter.
    
    Args:
        name: Name of the experiment
        base_params: Base parameters (txns, max_ops, max_key, read_only)
        varying_param: Name of the parameter to vary
        param_values: List of values for the varying parameter
        cases: Number of workload cases per configuration
        
    Returns:
        True if successful, False otherwise
    """
    print(f"\n{YELLOW}{'='*60}{NC}")
    print(f"{YELLOW}Experiment: {name}{NC}")
    print(f"{YELLOW}{'='*60}{NC}")
    print(f"Base parameters: {base_params}")
    print(f"Varying parameter: {varying_param}")
    print(f"Values to test: {param_values}")
    print()
    
    for value in param_values:
        params = base_params.copy()
        params[varying_param] = value
        
        print(f"{CYAN}[{varying_param}={value}]{NC}")
        
        # Generate workload
        if not generate_workload(
            txns=params['txns'],
            max_ops=params['max_ops'],
            max_key=params['max_key'],
            read_only=params['read_only'],
            cases=cases
        ):
            print(f"{RED}✗ Failed to generate workload{NC}")
            return False
        
        print()
    
    return True

def main():
    """Main execution"""
    project_dir = get_project_dir()
    os.chdir(project_dir)
    
    print("=" * 60)
    print("Allocation Random Performance Experiment")
    print("=" * 60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Base parameters (baseline values)
    base_params = {
        'txns': 500,
        'max_ops': 10,
        'max_key': 300,
        'read_only': 50
    }
    
    cases = 3  # Number of cases per configuration
    
    print("Base Parameters (Baseline):")
    print(f"  txns: {base_params['txns']}")
    print(f"  max_ops: {base_params['max_ops']}")
    print(f"  max_key: {base_params['max_key']}")
    print(f"  read_only: {base_params['read_only']}%")
    print(f"  cases: {cases}")
    print()
    
    # Define experiments (using control variable method)
    experiments = [
        {
            'name': 'Exp1: Varying Transaction Count',
            'varying_param': 'txns',
            'values': [100, 200, 500]
        },
        {
            'name': 'Exp2: Varying Operations per Transaction',
            'varying_param': 'max_ops',
            'values': [10, 15, 20, 30, 40, 60, 80, 100]
        },
        {
            'name': 'Exp3: Varying Key Space Size',
            'varying_param': 'max_key',
            'values': [100, 500, 600, 700, 800, 900, 1000]
        },
        {
            'name': 'Exp4: Varying Read-Only Percentage',
            'varying_param': 'read_only',
            'values': [0, 20, 40, 60, 80, 100]
        }
    ]
    
    # Run all experiments
    all_success = True
    for exp in experiments:
        if not run_experiment(
            name=exp['name'],
            base_params=base_params,
            varying_param=exp['varying_param'],
            param_values=exp['values'],
            cases=cases
        ):
            print(f"{RED}✗ Experiment failed: {exp['name']}{NC}")
            all_success = False
            break
    
    # Final summary
    print()
    print("=" * 60)
    print("Experiment Summary")
    print("=" * 60)
    
    random_workload_dir = project_dir / 'data' / 'random_workload'
    if random_workload_dir.exists():
        workload_files = list(random_workload_dir.glob('*.json'))
        print(f"{GREEN}✓ Workload generation completed{NC}")
        print(f"Total workload files generated: {len(workload_files)}")
        print(f"Output directory: {random_workload_dir}")
    
    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    if all_success:
        print(f"{GREEN}✓ All experiments completed successfully!{NC}")
        return 0
    else:
        print(f"{RED}✗ Some experiments failed{NC}")
        return 1

if __name__ == '__main__':
    sys.exit(main())
