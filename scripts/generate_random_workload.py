#!/usr/bin/env python3
"""
Generate random KV operation workloads.
Generates random transaction workloads with configurable parameters.
Output files are named: workload_{threads}t_{max_ops}o_{max_keys}k_{cases}r_{case_num}.json
"""

import os
import sys
import json
import random
import argparse
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

class RandomWorkloadGenerator:
    """Generate random KV operation workloads"""
    
    def __init__(self, total_txns: int, max_ops: int, max_key: int, cases: int = 1, read_only_percent: int = 0):
        """
        Initialize the random workload generator.
        
        Args:
            total_txns: Total number of transactions
            max_ops: Maximum number of operations per transaction
            max_key: Maximum key ID (1 to max_key)
            cases: Number of different workload cases to generate
            read_only_percent: Percentage of read-only transactions (0-100)
        """
        self.total_txns = total_txns
        self.max_ops = max_ops
        self.max_key = max_key
        self.cases = cases
        self.read_only_percent = read_only_percent
        self.random = random.Random()
        
    def get_project_dir(self) -> Path:
        """Get project root directory"""
        return Path(__file__).parent.parent.absolute()
    
    def generate_read_only_operations(self) -> List[Dict]:
        """Generate read-only operations for a transaction"""
        num_ops = self.random.randint(1, self.max_ops)
        operations = []
        
        for op_id in range(1, num_ops + 1):
            key_num = self.random.randint(1, self.max_key)
            operations.append({
                'id': op_id,
                'type': 'READ',
                'key': f'key_{key_num}'
            })
        
        return operations
    
    def generate_random_operations(self) -> List[Dict]:
        """Generate random operations for a transaction"""
        num_ops = self.random.randint(1, self.max_ops)
        operations = []
        
        for op_id in range(1, num_ops + 1):
            op_type = self.random.choice(['READ', 'WRITE'])
            key_num = self.random.randint(1, self.max_key)
            
            if op_type == 'UPDATE':
                # UPDATE is represented as both READ and WRITE
                operations.append({
                    'id': op_id,
                    'type': 'READ',
                    'key': f'key_{key_num}'
                })
                operations.append({
                    'id': op_id,
                    'type': 'WRITE',
                    'key': f'key_{key_num}'
                })
            else:
                operations.append({
                    'id': op_id,
                    'type': op_type,
                    'key': f'key_{key_num}'
                })
        
        return operations
    
    def generate_transactions(self) -> List[Dict]:
        """Generate random transactions"""
        transactions = []
        
        for txn_id in range(1, self.total_txns + 1):
            # Determine if this transaction should be read-only
            is_read_only = self.random.randint(1, 100) <= self.read_only_percent
            
            if is_read_only:
                operations = self.generate_read_only_operations()
            else:
                operations = self.generate_random_operations()
            
            # Set isolation level to SERIALIZABLE
            isolation_level = 'SERIALIZABLE'
            
            transactions.append({
                'name': f'Txn_{txn_id}',
                'isolationLevel': isolation_level,
                'operations': operations
            })
        
        return transactions
    
    def generate_workloads(self):
        """Generate random workloads"""
        project_dir = self.get_project_dir()
        random_workload_dir = project_dir / 'data' / 'random_workload'
        
        print("=" * 60)
        print("Random Workload Generator")
        print("=" * 60)
        print()
        print(f"Configuration:")
        print(f"  Total Transactions: {self.total_txns}")
        print(f"  Max operations per transaction: {self.max_ops}")
        print(f"  Max key ID: {self.max_key}")
        print(f"  Number of cases: {self.cases}")
        print(f"  Read-only transaction percentage: {self.read_only_percent}%")
        print()
        
        random_workload_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate filename based on parameters
        # Format: workload_{threads}t_{max_ops}o_{max_keys}k_{cases}r_{case_num}.json
        max_key_str = f"{self.max_key}k" if self.max_key == 0 else f"{self.max_key}k"
        
        total_generated = 0
        
        for case_num in range(1, self.cases + 1):
            print(f"{CYAN}Generating case {case_num}/{self.cases}...{NC}")
            
            try:
                transactions = self.generate_transactions()
                
                # Shuffle transactions
                self.random.shuffle(transactions)
                
                # Create workload
                workload = {'templates': transactions}
                
                # Generate filename
                filename = f"workload_{self.total_txns}t_{self.max_ops}o_{max_key_str}_{self.read_only_percent}r_{case_num}.json"
                output_file = random_workload_dir / filename
                
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(workload, f, indent=2)
                
                print(f"{GREEN}✓{NC} Generated {output_file.name}")
                print(f"  - Transactions: {len(transactions)}")
                print(f"  - Output size: {output_file.stat().st_size} bytes")
                total_generated += 1
                
            except Exception as e:
                print(f"{RED}✗{NC} Error generating case {case_num}: {e}")
                return False
        
        print()
        print("=" * 60)
        print("Generation Complete")
        print("=" * 60)
        print(f"Total files generated: {total_generated}")
        print(f"Output directory: {random_workload_dir}")
        print()
        
        return True

def main():
    parser = argparse.ArgumentParser(
        description='Generate random KV operation workloads',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Generate 1 workload with 500 transactions, max 10 ops, 500k keys
  python generate_random_workload.py --txns 500 --max-ops 10 --max-key 500000 --cases 1
  
  # Generate 50 workloads for benchmark
  python generate_random_workload.py --txns 500 --max-ops 10 --max-key 500000 --cases 50
  
  # Generate workloads with 30% read-only transactions
  python generate_random_workload.py --txns 500 --max-ops 10 --max-key 500000 --cases 5 --read-only 30
        '''
    )
    
    parser.add_argument(
        '--txns',
        type=int,
        default=1000,
        help='Total number of transactions (default: 1000)'
    )
    
    parser.add_argument(
        '-o', '--max-ops',
        type=int,
        default=10,
        help='Maximum number of operations per transaction (default: 10)'
    )
    
    parser.add_argument(
        '-k', '--max-key',
        type=int,
        default=500000,
        help='Maximum key ID (1 to max_key) (default: 500000)'
    )
    
    parser.add_argument(
        '-c', '--cases',
        type=int,
        default=1,
        help='Number of different workload cases to generate (default: 1)'
    )
    
    parser.add_argument(
        '-r', '--read-only',
        type=int,
        default=0,
        help='Percentage of read-only transactions (0-100) (default: 0)'
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.txns <= 0:
        print(f"{RED}Error: txns must be positive{NC}")
        return 1
    
    if args.max_ops <= 0:
        print(f"{RED}Error: max_ops must be positive{NC}")
        return 1
    
    if args.max_key <= 0:
        print(f"{RED}Error: max_key must be positive{NC}")
        return 1
    
    if args.cases <= 0:
        print(f"{RED}Error: cases must be positive{NC}")
        return 1
    
    if args.read_only < 0 or args.read_only > 100:
        print(f"{RED}Error: read-only percentage must be between 0 and 100{NC}")
        return 1
    
    generator = RandomWorkloadGenerator(
        total_txns=args.txns,
        max_ops=args.max_ops,
        max_key=args.max_key,
        read_only_percent=args.read_only,
        cases=args.cases,
    )
    
    success = generator.generate_workloads()
    return 0 if success else 1

if __name__ == '__main__':
    sys.exit(main())
