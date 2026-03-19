# Artifact for `On Mixing Database Isolation Levels`

## Directory Organization

```plain
/MixIso
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
java -cp "target/classes;target/dependency/*" algorithm.BenchWorkloadGenerator --sessions 10 --txns-per-session 2 --max-key 50 --cases 3

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

## Evaluation

This section describes how to reproduce the evaluation from the paper "On Mixing Database Isolation Levels".

### Research Questions

The evaluation addresses three key research questions:

1. **Q1: Effectiveness** — Can our allocator safely allocate weaker isolation levels than existing approaches while preserving serializability?

2. **Q2: Efficiency** — How efficient is our allocator in computing isolation level allocations on large workloads?

3. **Q3: System Performance** — To what extent do the weaker isolation levels assigned by our allocator translate into performance gains in database systems?

### Benchmarks

We use three representative OLTP benchmarks:

- **SmallBank**: Bank transaction workload with balance queries, deposits, and transfers
- **TPC-C**: Industry standard wholesale distribution business workload
- **Courseware**: University course enrollment system workload

### Reproduce Experiments

#### Q1: Benchmark Workload Allocation & Execution

Use **Task A** to generate benchmark workloads, allocate isolation levels, and execute them with distributed simulation:

```sh
# 1) Generate benchmark workloads
java -cp "target/classes;target/dependency/*" algorithm.BenchWorkloadGenerator --sessions 3 --txns-per-session 100 --max-key 50 --cases 3

# 2) Allocate isolation levels in batch
java -cp "target/classes;target/dependency/*" algorithm.BenchWorkloadAllocatorBatch

# 3) Execute allocated benchmark workloads with distributed simulation
java -cp "target/classes;target/dependency/*" algorithm.BenchWorkloadExecutorBatch 5 100 300 data/bench_execution_results.csv
```

**Output**: Distribution of isolation levels across program instances in each benchmark at `data/bench_allocation_distribution.csv`.

#### Q2: Allocation Efficiency on Random Workloads

Use **Task B** and **Task C** to evaluate the allocator's scalability by varying workload parameters:

```sh
# 1) Generate random workloads with default parameters
java -cp "target/classes;target/dependency/*" algorithm.RandomWorkloadGenerator --txns 500 --max-ops 10 --max-key 500000 --read-only 30 --cases 5

# 2) Allocate all random workloads and measure timing
java -cp "target/classes;target/dependency/*" algorithm.RandomWorkloadAllocatorBatch

# 3) Run controlled experiment sweep with parameter variations
java -cp "target/classes;target/dependency/*" algorithm.RandomWorkloadExperiment
```

**Output**: Allocation timing and performance metrics at `data/allocation_performance.csv` and `data/allocation_performance_analysis.csv`.

To measure scalability across different parameters:
- Vary transaction count by adjusting `--txns` parameter
- Vary operations per transaction by adjusting `--max-ops` parameter
- Vary key space size by adjusting `--max-key` parameter

#### Q3: System Performance with Mixed Isolation Levels

Execute **Task A** (specifically step 3) to measure throughput and latency improvements from fine-grained isolation level allocation:

```sh
java -cp "target/classes;target/dependency/*" algorithm.BenchWorkloadExecutorBatch 5 100 300 data/bench_execution_results.csv
```

This executes the allocated workloads in a simulated distributed setting (5 data centers with 100–300ms WAN latencies) and reports throughput and latency metrics.

**Output**: Execution statistics at `data/bench_execution_results.csv`, including:
- Throughput (transactions per second)
- Latency (average transaction completion time)
- Commit/abort rates per isolation level
