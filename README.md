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
- **Output**: `data/allocated_bench_workload/`

```sh
# Generate benchmark workloads
python scripts/generate_bench_workload.py --txns 1000 --max-key 500 --cases 100

# Perform allocation
python scripts/allocate_bench_workload.py
```

### Q2: Random Workload Allocation & Performance Analysis

This experiment explores the relationship between workload parameters and allocation performance.

- **Scripts**: `scripts/generate_random_workload.py`, `scripts/random_workload_for_test.py`, `scripts/allocate_random_workload.py`
- **Output**: `data/allocated_random_workload/`, `data/allocation_performance_analysis.csv`

```sh
# Generate random workloads
python scripts/generate_random_workload.py --txns 500 --max-ops 10 --max-key 500 --read-only 50 --cases 5

# Preprocess for testing
python scripts/random_workload_for_test.py

# Perform allocation
python scripts/allocate_random_workload.py
```

Results are saved in the `data/` directory. You can check `data/allocation_performance_analysis.csv` for detailed timing information.
