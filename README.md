# Artifact and Technical Report for `On Mixing Database Isolation Levels`

Technical Report: [`tech-rpt.pdf`](tech-rpt.pdf).

## Directory Organization

```plain
/MixIso
|-- tech-rpt.pdf            # the accompanying technical report
|-- README.md
|-- pom.xml                 # Maven project configuration
|-- src/                    # Java source code for MixIso allocator
|-- scripts/                # Python scripts for data generation and experiment orchestration
|   |-- generate_bench_workload.py
|   |-- allocate_bench_workload.py
|   |-- generate_random_workload.py
|   |-- random_workload_for_test.py
|   `-- allocate_random_workload.py
`-- data/                   # Workload files and experimental results
    |-- bench_workload/     # Base benchmark workloads (TPC-C, SmallBank, and Courseware) for Q1
    |-- random_workload/    # Base random workloads for Q2
    |-- allocated_bench_workload/  # Results of Q1
    `-- allocated_random_workload/ # Results of Q2
```

## Reuse MixIso

### Prerequisite

- **Java 17** or above is recommended.
- **Maven** for building the Java project.
- **Python 3.10+** is optional (legacy scripts only).

### Build with Maven

```sh
mvn clean package
```

> On Windows PowerShell, keep the classpath separator `;`.
> On Linux/macOS shells, replace it with `:`.

### Core Usage

The core allocation logic is implemented in Java. You can run it directly:

```sh
# mode 1: benchmark performance
java -cp "target/classes;target/dependency/*" algorithm.Allocator benchmark <workload_file> <output_csv> [warmups] [iterations]

# mode 2: allocate isolation levels
java -cp "target/classes;target/dependency/*" algorithm.Allocator allocate <input_workload> <output_workload>
```

## Java CLI Workflows (Python-free)

All major automation workflows previously in `scripts/*.py` now have Java CLI equivalents:

```sh
# 1) Generate benchmark workloads (replaces generate_bench_workload.py)
java -cp "target/classes;target/dependency/*" algorithm.BenchWorkloadGenerator --sessions 3 --txns-per-session 100 --max-key 50 --cases 3

# 2) Batch allocate benchmark workloads (replaces allocate_bench_workload.py)
java -cp "target/classes;target/dependency/*" algorithm.BenchWorkloadAllocatorBatch
# -> outputs: data/bench_allocation_distribution.csv and data/bench_allocation_distribution.png

# 3) Batch execute allocated benchmark workloads (replaces execute_allocated_bench.py)
java -cp "target/classes;target/dependency/*" algorithm.BenchWorkloadExecutorBatch 5 100 300 data/bench_execution_results.csv

# 4) Generate random workloads (replaces generate_random_workload.py)
java -cp "target/classes;target/dependency/*" algorithm.RandomWorkloadGenerator --txns 500 --max-ops 10 --max-key 500000 --read-only 30 --cases 5

# 5) Batch allocate random workloads with timing CSV (replaces allocate_random_workload.py)
java -cp "target/classes;target/dependency/*" algorithm.RandomWorkloadAllocatorBatch
# -> outputs: data/allocation_performance.csv, data/allocation_performance_analysis.csv, data/allocation_performance.png

# 6) Run random workload experiment orchestration (replaces random_workload_for_test.py)
java -cp "target/classes;target/dependency/*" algorithm.RandomWorkloadExperiment
```

## Task Guide (Recommended)

This section shows how to complete common tasks end-to-end using Java only.

### Task A: Benchmark workload allocation + execution (Q1)

1. Generate benchmark workloads:

```sh
java -cp "target/classes;target/dependency/*" algorithm.BenchWorkloadGenerator --sessions 3 --txns-per-session 100 --max-key 50 --cases 3
```

2. Allocate isolation levels in batch (also exports Figure-8 style distribution files):

```sh
java -cp "target/classes;target/dependency/*" algorithm.BenchWorkloadAllocatorBatch
```

3. Execute all allocated benchmark files with distributed simulation:

```sh
java -cp "target/classes;target/dependency/*" algorithm.BenchWorkloadExecutorBatch 5 100 300 data/bench_execution_results.csv
```

Outputs:
- `data/allocated_bench_workload/`
- `data/bench_allocation_distribution.csv`
- `data/bench_allocation_distribution.png`
- `data/bench_execution_results.csv`

### Task B: Random workload allocation + performance analysis (Q2)

1. Generate random workloads:

```sh
java -cp "target/classes;target/dependency/*" algorithm.RandomWorkloadGenerator --txns 500 --max-ops 10 --max-key 500000 --read-only 30 --cases 5
```

2. Allocate all random workloads and export timing/analysis/plot:

```sh
java -cp "target/classes;target/dependency/*" algorithm.RandomWorkloadAllocatorBatch
```

Outputs:
- `data/allocated_random_workload/`
- `data/allocation_performance.csv`
- `data/allocation_performance_analysis.csv`
- `data/allocation_performance.png`

### Task C: Full random experiment sweep (control-variable method)

```sh
java -cp "target/classes;target/dependency/*" algorithm.RandomWorkloadExperiment
```

Output:
- `data/allocation_experiment_summary.csv`

## Legacy Python Scripts (Optional)

Python scripts in `scripts/` are kept for compatibility and quick prototyping.
The Java CLI workflow above is the primary and recommended path.

## Reproduce Experiments

### Q1: Benchmark Workload Allocation

Use **Task A** in the section above.

### Q2: Random Workload Allocation & Performance Analysis

Use **Task B** and **Task C** in the section above.
