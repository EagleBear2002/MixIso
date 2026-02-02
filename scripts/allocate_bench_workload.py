#!/usr/bin/env python3
"""
Fast WSL allocation script using Python
Allocates isolation levels to benchmark workload files in parallel
"""

import os
import sys
import json
import subprocess
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
        print(f"Completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        return 0

if __name__ == '__main__':
    sys.exit(main())
