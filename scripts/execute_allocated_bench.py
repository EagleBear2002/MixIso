#!/usr/bin/env python3
"""
Execute allocated benchmark workloads using Java Executor.
Runs all JSON files in data/allocated_bench_workload and appends metrics to CSV.
"""

import os
import sys
import subprocess
import argparse
from pathlib import Path
from datetime import datetime


if sys.platform == 'win32':
	sys.stdout.reconfigure(encoding='utf-8')


GREEN = '\033[0;32m'
RED = '\033[0;31m'
YELLOW = '\033[1;33m'
CYAN = '\033[0;36m'
NC = '\033[0m'


def get_project_dir() -> Path:
	return Path(__file__).parent.parent.absolute()


def get_classpath(classes_dir: Path, project_dir: Path):
	classpath_parts = [str(classes_dir)]

	dependency_dir = project_dir / 'target' / 'dependency'
	if dependency_dir.exists():
		classpath_parts.append(str(dependency_dir / '*'))

	m2_repo = Path.home() / '.m2' / 'repository'
	if m2_repo.exists():
		required_deps = [
			'com/fasterxml/jackson/core/jackson-databind/2.13.3/jackson-databind-2.13.3.jar',
			'com/fasterxml/jackson/core/jackson-core/2.13.3/jackson-core-2.13.3.jar',
			'com/fasterxml/jackson/core/jackson-annotations/2.13.3/jackson-annotations-2.13.3.jar',
		]
		for dep in required_deps:
			dep_path = m2_repo / dep
			if dep_path.exists():
				classpath_parts.append(str(dep_path))

	return classpath_parts


def run_executor(input_file: Path, output_csv: Path, classpath: str, dc_count: int, min_rtt: int, max_rtt: int, debug: bool = False):
	filename = input_file.name

	try:
		java_cmd = 'java'
		java_home = os.environ.get('JAVA_HOME')
		if java_home:
			candidate = Path(java_home) / 'bin' / ('java.exe' if sys.platform == 'win32' else 'java')
			if candidate.exists():
				java_cmd = str(candidate)

		cmd = [
			java_cmd,
			'-cp', classpath,
			'algorithm.Executor',
			str(input_file),
			str(output_csv),
			str(dc_count),
			str(min_rtt),
			str(max_rtt),
		]

		result = subprocess.run(
			cmd,
			capture_output=True,
			encoding='utf-8',
			timeout=600,
		)

		if result.returncode == 0:
			return (filename, True, None)

		error_msg = (result.stderr or result.stdout or 'unknown error').strip()
		if debug:
			print(f"{YELLOW}Debug output for {filename}:{NC}\n{error_msg}")
		return (filename, False, error_msg[:500])
	except Exception as e:
		return (filename, False, str(e))


def main():
	parser = argparse.ArgumentParser(description='Execute all allocated benchmark workload files')
	parser.add_argument('--dc-count', type=int, default=5, help='Number of simulated data centers')
	parser.add_argument('--min-rtt', type=int, default=100, help='Minimum WAN RTT in milliseconds')
	parser.add_argument('--max-rtt', type=int, default=300, help='Maximum WAN RTT in milliseconds')
	parser.add_argument('--output', default='data/bench_execution_results.csv', help='CSV output path')
	parser.add_argument('--debug', action='store_true', help='Show full error output when failed')
	args = parser.parse_args()

	if args.dc_count <= 0:
		print(f"{RED}Error: dc-count must be positive{NC}")
		return 1
	if args.min_rtt < 0:
		print(f"{RED}Error: min-rtt must be >= 0{NC}")
		return 1
	if args.max_rtt < args.min_rtt:
		print(f"{RED}Error: max-rtt must be >= min-rtt{NC}")
		return 1
	project_dir = get_project_dir()
	os.chdir(project_dir)

	allocated_dir = project_dir / 'data' / 'allocated_bench_workload'
	classes_dir = project_dir / 'target' / 'classes'
	output_csv = project_dir / args.output

	print('=' * 52)
	print('Allocated Benchmark Workload Executor')
	print('=' * 52)
	print()

	if not allocated_dir.exists():
		print(f"{RED}Error: allocated workload directory not found: {allocated_dir}{NC}")
		return 1

	print(f"{CYAN}Recompiling Java code...{NC}")
	try:
		result = subprocess.run(
			['mvn', 'clean', 'compile', 'dependency:copy-dependencies'],
			capture_output=True,
			encoding='utf-8',
			timeout=300,
		)
		if result.returncode == 0:
			print(f"{GREEN}✓ Java code compiled successfully{NC}")
		else:
			print(f"{YELLOW}Warning: Maven compilation failed, fallback to existing classes{NC}")
	except Exception as e:
		print(f"{YELLOW}Warning: Failed to run Maven: {e}{NC}")
		print(f"{YELLOW}Fallback to existing classes{NC}")

	print()

	if not classes_dir.exists():
		print(f"{RED}Error: classes directory not found: {classes_dir}{NC}")
		return 1

	workload_files = sorted(allocated_dir.glob('*.json'))
	if not workload_files:
		print(f"{RED}No allocated workload files found in {allocated_dir}{NC}")
		return 1

	classpath_parts = get_classpath(classes_dir, project_dir)
	classpath = ';'.join(classpath_parts) if sys.platform.startswith('win') else ':'.join(classpath_parts)

	print(f"Found {len(workload_files)} files")
	print(f"Output CSV: {output_csv}")
	print(f"DC count: {args.dc_count}, RTT: [{args.min_rtt}, {args.max_rtt}] ms")
	print(f"Start at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
	print()

	success_count = 0
	failed_count = 0
	failed_files = []

	for idx, input_file in enumerate(workload_files, 1):
		filename, ok, error = run_executor(
			input_file,
			output_csv,
			classpath,
			args.dc_count,
			args.min_rtt,
			args.max_rtt,
			args.debug,
		)

		if ok:
			success_count += 1
			print(f"[{idx:3d}/{len(workload_files)}] {filename:<40} {GREEN}✓{NC}")
		else:
			failed_count += 1
			failed_files.append((filename, error))
			print(f"[{idx:3d}/{len(workload_files)}] {filename:<40} {RED}✗{NC}")

	print()
	print('=' * 52)
	print('Execution Summary')
	print('=' * 52)
	print(f"Processed: {len(workload_files)}")
	print(f"Succeeded: {GREEN}{success_count}{NC}")
	print(f"Failed: {RED}{failed_count}{NC}")

	if failed_files:
		print('\nFailed files:')
		for filename, error in failed_files:
			print(f"  - {filename}: {error}")
		return 1

	print(f"\n{GREEN}All workloads executed successfully.{NC}")
	print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
	return 0


if __name__ == '__main__':
	sys.exit(main())

