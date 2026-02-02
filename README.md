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
- **Python 3.10+** for running automation scripts.

### Build with Maven

```sh
mvn clean package
```

### Usage

The core allocation logic is implemented in Java. You can run it directly:

```sh
# mode 1: benchmark performance
java -cp "target/classes;target/dependency/*" algorithm.Allocator benchmark <workload_file> <output_csv> [warmups] [iterations]

# mode 2: allocate isolation levels
java -cp "target/classes;target/dependency/*" algorithm.Allocator allocate <input_workload> <output_workload>
```

Alternatively, use the provided Python scripts in the `scripts/` directory which handle classpath resolution and parallel execution.

## Reproduce Experiments

### Q1: Benchmark Workload Allocation

This experiment allocates isolation levels for standard benchmarks (Courseware, SmallBank, TPC-C).

- **Scripts**: `scripts/generate_bench_workload.py`, `scripts/allocate_bench_workload.py`
- **Output**: `data/allocated_bench_workload/`, `data/bench_allocation_distribution.png`, `data/bench_allocation_distribution.csv`
- **Visualization**: The script generates a **stacked bar chart** (`bench_allocation_distribution.png`) showing the distribution of isolation levels (SER, SI, PSI, PC, RA) across all benchmark instances, corresponding to **Figure 8** in the technical report.

```sh
# Generate benchmark workloads
python scripts/generate_bench_workload.py --txns 1000 --max-key 500 --cases 100

# Perform allocation (includes visualization generation)
python scripts/allocate_bench_workload.py
```

The generated visualization provides insights into:
- How different benchmarks (SmallBank, Courseware, TPC-C) have distinct isolation level distributions
- The consistency of allocation decisions across multiple workload instances
- The preference patterns for different isolation levels in each benchmark

### Q2: Random Workload Allocation & Performance Analysis

This experiment explores the relationship between workload parameters and allocation performance.

- **Scripts**: `scripts/generate_random_workload.py`, `scripts/random_workload_for_test.py`, `scripts/allocate_random_workload.py`
- **Output**: `data/allocated_random_workload/`, `data/allocation_performance.png`, `data/allocation_performance_analysis.csv`
- **Visualization**: The script generates a **three-panel performance chart** (`allocation_performance.png`) showing how allocation performance varies with key workload parameters (number of transactions, max operations per transaction, and max key ID), corresponding to **Figure 9** in the technical report.

```sh
# Generate random workloads
python scripts/generate_random_workload.py --txns 500 --max-ops 10 --max-key 500 --read-only 50 --cases 5

# Preprocess for testing (includes visualization generation)
python scripts/random_workload_for_test.py

# Perform allocation
python scripts/allocate_random_workload.py
```

The generated visualization provides insights into:
- Performance trends across different transaction counts
- Impact of operations complexity on allocation efficiency
- Relationship between key space size and allocation quality

Results are saved in the `data/` directory. You can check `data/allocation_performance_analysis.csv` for detailed timing information.
