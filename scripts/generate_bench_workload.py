#!/usr/bin/env python3
"""
Generate benchmark workloads from templates.
Reads JSON files from data/benchmarks and generates workload files to data/bench_workload.
Each transaction is named "templatename_id", e.g., "Balance_1".
Each workload contains multiple sessions, and each session contains multiple transactions.
"""

import os
import sys
import json
import random
import argparse
import subprocess
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

# Set UTF-8 encoding for Windows compatibility
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

# Colors
GREEN = '\033[0;32m'
RED = '\033[0;31m'
YELLOW = '\033[1;33m'
CYAN = '\033[0;36m'
NC = '\033[0m'

class WorkloadGenerator:
    """Generate benchmark workloads from templates"""
    
    def __init__(self, sessions: int, txns_per_session: int, max_key: int, cases: int):
        self.sessions = sessions
        self.txns_per_session = txns_per_session
        self.max_key = max_key
        self.cases = cases
        self.random = random.Random()

    @property
    def total_txns(self) -> int:
        return self.sessions * self.txns_per_session
        
    def get_project_dir(self) -> Path:
        """Get project root directory"""
        return Path(__file__).parent.parent.absolute()
    
    def instantiate_template(self, template_ops: List[Dict], param_values: Dict[str, int]) -> List[Dict]:
        """
        Convert template operations to concrete program instance operations.
        - Operations with parameters use the provided parameter values to determine instance numbers
          Example: READ Account with param "N1" where N1=5 becomes READ Account_5
        - UPDATE operations convert to READ + WRITE on same variable
        - Parameters must be resolved from param_values - if a parameter is missing, it's an error
        - If params is a list, the values are concatenated with "_"
        """
        concrete_ops = []
        
        for op in template_ops:
            table = op['key']
            op_id = op['id']
            op_type = op['type']
            param_ref = op.get('params')  # Can be string or list
            
            # Determine the instance number(s)
            instance_nums = []
            if param_ref is not None:
                # Handle both string and list param references
                if isinstance(param_ref, list):
                    # Multiple parameters - get all their values
                    for param_name in param_ref:
                        if param_name not in param_values:
                            raise ValueError(f"Parameter '{param_name}' not found in param_values: {param_values.keys()}")
                        instance_nums.append(str(param_values[param_name]))
                else:
                    # Single parameter
                    if param_ref not in param_values:
                        raise ValueError(f"Parameter '{param_ref}' not found in param_values: {param_values.keys()}")
                    instance_nums.append(str(param_values[param_ref]))
                
                # Create concrete key by joining instance numbers
                concrete_key = table + "_" + "_".join(instance_nums)
            else:
                # No parameter reference, use random
                instance_num = self.random.randint(1, self.max_key)
                concrete_key = f"{table}_{instance_num}"
            
            # Handle UPDATE as both READ and WRITE on the same variable
            if op_type == 'UPDATE':
                concrete_ops.append({
                    'id': op_id,
                    'type': 'READ',
                    'key': concrete_key
                })
                concrete_ops.append({
                    'id': op_id,
                    'type': 'WRITE',
                    'key': concrete_key
                })
            else:
                # For READ and other operations
                concrete_ops.append({
                    'id': op_id,
                    'type': op_type,
                    'key': concrete_key
                })
        
        return concrete_ops
    
    def generate_from_benchmarks(self):
        """Generate workloads from benchmark templates"""
        project_dir = self.get_project_dir()
        benchmark_dir = project_dir / 'data' / 'benchmarks'
        bench_workload_dir = project_dir / 'data' / 'bench_workload'
        
        print("=" * 50)
        print("Benchmark Workload Generator")
        print("=" * 50)
        print()
        
        # Recompile Java code
        print(f"{CYAN}Recompiling Java code...{NC}")
        try:
            result = subprocess.run(
                ['mvn', 'clean', 'compile'],
                cwd=str(project_dir),
                capture_output=True,
                encoding='utf-8',
                timeout=300
            )
            if result.returncode != 0:
                error_msg = result.stderr
                print(f"{RED}Warning: Maven compilation failed{NC}")
                print(f"{YELLOW}Attempting to continue with existing build...{NC}")
            else:
                print(f"{GREEN}✓ Java code compiled successfully{NC}")
        except Exception as e:
            print(f"{RED}Warning: Failed to run Maven: {e}{NC}")
            print(f"{YELLOW}Attempting to continue with existing build...{NC}")
        
        print()
        
        if not benchmark_dir.exists():
            print(f"{RED}Error: Benchmark directory not found: {benchmark_dir}{NC}")
            return False
        
        bench_workload_dir.mkdir(parents=True, exist_ok=True)
        
        # Get all benchmark JSON files
        benchmark_files = sorted(benchmark_dir.glob('*.json'))
        
        if not benchmark_files:
            print(f"{RED}No benchmark files found in {benchmark_dir}{NC}")
            return False
        
        print(f"Found {len(benchmark_files)} benchmark files")
        print(f"Generating {self.cases} workload cases per benchmark")
        print()
        
        total_generated = 0
        
        for benchmark_file in benchmark_files:
            benchmark_name = benchmark_file.stem
            
            try:
                with open(benchmark_file, 'r', encoding='utf-8') as f:
                    benchmark_data = json.load(f)
                
                templates = benchmark_data.get('templates', [])
                
                if not templates:
                    print(f"{YELLOW}Warning: No templates in {benchmark_name}{NC}")
                    continue
                
                # Generate workload for each case
                for case_num in range(1, self.cases + 1):
                    # Set seed for reproducible but distinct workloads across cases
                    # Ensures consistent distribution regardless of when/how many times script is run
                    self.random.seed(case_num)
                    
                    transactions = []
                    template_counters = {}

                    # Use weighted random template selection to generate exactly total_txns transactions
                    weighted_templates = []
                    for template in templates:
                        weighted_templates.append({
                            'template': template,
                            'weight': max(0.0, float(template.get('percentage', 0.0)))
                        })

                    total_weight = sum(item['weight'] for item in weighted_templates)
                    if total_weight <= 0:
                        # Fallback to uniform distribution when percentages are invalid
                        for item in weighted_templates:
                            item['weight'] = 1.0
                        total_weight = float(len(weighted_templates))

                    cumulative_weights = []
                    running = 0.0
                    for item in weighted_templates:
                        running += item['weight']
                        cumulative_weights.append(running)

                    for _ in range(self.total_txns):
                        pick = self.random.uniform(0, total_weight)
                        selected_idx = 0
                        while selected_idx < len(cumulative_weights) and pick > cumulative_weights[selected_idx]:
                            selected_idx += 1
                        if selected_idx >= len(weighted_templates):
                            selected_idx = len(weighted_templates) - 1

                        template = weighted_templates[selected_idx]['template']
                        name = template.get('name', 'unknown')
                        isolation_level = template.get('isolationLevel', 'SERIALIZABLE')
                        operations = template.get('operations', [])
                        params = template.get('params', [])

                        template_counters[name] = template_counters.get(name, 0) + 1

                        # Generate random parameter values for this instance
                        param_values = {}
                        for param_name in params:
                            param_values[param_name] = self.random.randint(1, self.max_key)

                        txn_name = f"{name}_{template_counters[name]}"
                        concrete_ops = self.instantiate_template(operations, param_values)

                        transactions.append({
                            'name': txn_name,
                            'isolationLevel': isolation_level,
                            'operations': concrete_ops
                        })
                    
                    # Shuffle transactions
                    self.random.shuffle(transactions)

                    # Split into sessions (each session has txns_per_session transactions)
                    sessions = []
                    for session_idx in range(self.sessions):
                        start = session_idx * self.txns_per_session
                        end = start + self.txns_per_session
                        sessions.append({
                            'id': session_idx + 1,
                            'transactions': transactions[start:end]
                        })
                    
                    # Save workload
                    workload = {'sessions': sessions}
                    output_file = bench_workload_dir / (
                        f"{benchmark_name}_{self.sessions}s_{self.txns_per_session}t_"
                        f"{self.max_key}k_{case_num}.json"
                    )
                    
                    with open(output_file, 'w', encoding='utf-8') as f:
                        json.dump(workload, f, indent=2)
                    
                    print(f"{GREEN}✓{NC} Generated {output_file.name}")
                    total_generated += 1
                
            except Exception as e:
                print(f"{RED}✗{NC} Error processing {benchmark_name}: {e}")
                return False
        
        print()
        print("=" * 50)
        print("Generation Complete")
        print("=" * 50)
        print(f"Total files generated: {total_generated}")
        print(f"Output directory: {bench_workload_dir}")
        print()
        
        return True

def main():
    parser = argparse.ArgumentParser(
        description='Generate benchmark workloads from templates',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Generate 1 workload: 10 sessions, 1000 txns per session, 100 max keys
  python generate_bench_workload.py --sessions 10 --txns-per-session 1000 --max-key 100 --cases 1
  
  # Generate 20 workloads for benchmark
  python generate_bench_workload.py --sessions 10 --txns-per-session 1000 --max-key 100 --cases 20
        '''
    )
    
    parser.add_argument(
        '--sessions',
        type=int,
        default=3,
        help='Number of sessions in each workload'
    )

    parser.add_argument(
        '--txns-per-session',
        type=int,
        default=100,
        help='Number of transactions per session'
    )
    
    parser.add_argument(
        '-k', '--max-key',
        type=int,
        default=50,
        help='Maximum key ID'
    )
    
    parser.add_argument(
        '-c', '--cases',
        type=int,
        default=3,
        help='Number of different workload cases to generate'
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.sessions <= 0:
        print(f"{RED}Error: sessions must be positive{NC}")
        return 1

    if args.txns_per_session <= 0:
        print(f"{RED}Error: txns_per_session must be positive{NC}")
        return 1
    
    if args.max_key <= 0:
        print(f"{RED}Error: max_key must be positive{NC}")
        return 1
    
    if args.cases <= 0:
        print(f"{RED}Error: cases must be positive{NC}")
        return 1
    
    generator = WorkloadGenerator(
        sessions=args.sessions,
        txns_per_session=args.txns_per_session,
        max_key=args.max_key,
        cases=args.cases
    )
    success = generator.generate_from_benchmarks()
    return 0 if success else 1

if __name__ == '__main__':
    sys.exit(main())
