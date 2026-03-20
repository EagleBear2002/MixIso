# MixIso 实验手册（当前版本）

本仓库是论文 *On Mixing Database Isolation Levels* 的实验代码实现，当前版本以 **Java CLI** 为主，支持：

- 基准工作负载生成
- 隔离级别分配
- 分布式执行仿真（默认低延迟）
- 三种执行策略性能对比（SER / SI-SER / PC-SI-SER）

## 1. 环境要求

- Java 17+
- Maven 3.8+

> Windows PowerShell 的 classpath 分隔符使用 `;`；Linux/macOS 使用 `:`。

## 2. 构建与依赖

在项目根目录执行：

```sh
mvn -DskipTests compile
mvn dependency:copy-dependencies
```

说明：

- `compile` 生成 `target/classes`
- `copy-dependencies` 生成 `target/dependency/*`，用于运行 `java -cp` 命令

## 3. 项目目录（实验相关）

```plain
MixIso
├─ src/main/java/algorithm/
│  ├─ Allocator.java
│  ├─ BenchWorkloadGenerator.java
│  ├─ BenchWorkloadAllocatorBatch.java
│  ├─ BenchWorkloadExecutorBatch.java
│  ├─ BenchStrategyComparisonBatch.java
│  ├─ RandomWorkloadGenerator.java
│  ├─ RandomWorkloadAllocatorBatch.java
│  ├─ RandomWorkloadExperiment.java
│  └─ VisualizationExporter.java
└─ data/
    ├─ bench_workload/
    ├─ allocated_bench_workload/
    ├─ benchmarks/
    └─ *.csv / *.png
```

## 4. 核心命令

### 4.1 单文件分配 / 评估

```sh
# 评估（benchmark 模式）
java -cp "target/classes;target/dependency/*" algorithm.Allocator benchmark <workload_file> <output_csv> [warmups] [iterations]

# 分配（allocate 模式）
java -cp "target/classes;target/dependency/*" algorithm.Allocator allocate <input_workload> <output_workload>
```

### 4.2 批处理工作流（推荐）

```sh
# 1) 生成基准工作负载
java -cp "target/classes;target/dependency/*" algorithm.BenchWorkloadGenerator --sessions 10 --txns-per-session 200 --max-key 50000 --cases 1

# 2) 批量分配隔离级别（并导出分布图）
java -cp "target/classes;target/dependency/*" algorithm.BenchWorkloadAllocatorBatch

# 3) 批量执行已分配工作负载（默认 5 个 DC，默认延迟 3-10ms）
java -cp "target/classes;target/dependency/*" algorithm.BenchWorkloadExecutorBatch
```

`BenchWorkloadExecutorBatch` 参数：

```plain
java ... algorithm.BenchWorkloadExecutorBatch [dcCount] [minRttMs] [maxRttMs] [outputCsv]
```

当前默认值：

- `dcCount=5`
- `minRttMs=3`
- `maxRttMs=10`
- `outputCsv=data/bench_execution_results.csv`

示例（显式指定）：

```sh
java -cp "target/classes;target/dependency/*" algorithm.BenchWorkloadExecutorBatch 5 3 10 data/bench_execution_results.csv
```

## 5. 三种策略对比实验（Q3 重点）

使用以下命令一次性完成三策略对比：

```sh
java -cp "target/classes;target/dependency/*" algorithm.BenchStrategyComparisonBatch
```

参数形式：

```plain
java ... algorithm.BenchStrategyComparisonBatch [dcCount] [minRttMs] [maxRttMs] [detailCsv] [summaryCsv] [outputPng]
```

当前默认值：

- `dcCount=5`
- `minRttMs=3`
- `maxRttMs=10`
- `detailCsv=data/bench_strategy_comparison_details.csv`
- `summaryCsv=data/bench_strategy_comparison_summary.csv`
- `outputPng=data/bench_strategy_comparison.png`

策略映射（代码内固定）：

- `SER`  ← `ExecutionStrategy.ALL_SER`
- `SI-SER` ← `ExecutionStrategy.NON_SER_AS_SI`
- `PC-SI-SER` ← `ExecutionStrategy.CURRENT`

输出文件：

- `data/bench_strategy_comparison_details.csv`：每个 workload × 每个策略的明细
- `data/bench_strategy_comparison_summary.csv`：按 benchmark 聚合后的均值
- `data/bench_strategy_comparison.png`：吞吐量/延迟对比图

## 6. 随机工作负载实验（Q2）

```sh
# 1) 生成随机工作负载
java -cp "target/classes;target/dependency/*" algorithm.RandomWorkloadGenerator --txns 500 --max-ops 10 --max-key 500000 --read-only 30 --cases 5

# 2) 批量分配并统计性能
java -cp "target/classes;target/dependency/*" algorithm.RandomWorkloadAllocatorBatch

# 3) 控制变量实验
java -cp "target/classes;target/dependency/*" algorithm.RandomWorkloadExperiment
```

常见输出：

- `data/allocation_performance.csv`
- `data/allocation_performance_analysis.csv`
- `data/allocation_performance.png`
- `data/allocation_experiment_summary.csv`

## 7. 一次性复现实验（推荐顺序）

```sh
# build
mvn -DskipTests compile
mvn dependency:copy-dependencies

# Q1: 分配效果
java -cp "target/classes;target/dependency/*" algorithm.BenchWorkloadGenerator --sessions 10 --txns-per-session 200 --max-key 50000 --cases 1
java -cp "target/classes;target/dependency/*" algorithm.BenchWorkloadAllocatorBatch

# Q3: 执行性能
java -cp "target/classes;target/dependency/*" algorithm.BenchWorkloadExecutorBatch

# Q3: 三策略横向对比
java -cp "target/classes;target/dependency/*" algorithm.BenchStrategyComparisonBatch

# Q2: 随机工作负载效率
java -cp "target/classes;target/dependency/*" algorithm.RandomWorkloadGenerator --txns 500 --max-ops 10 --max-key 500000 --read-only 30 --cases 5
java -cp "target/classes;target/dependency/*" algorithm.RandomWorkloadAllocatorBatch
java -cp "target/classes;target/dependency/*" algorithm.RandomWorkloadExperiment
```

## 8. 常见问题

1. **`ClassNotFoundException: com.fasterxml.jackson...`**
    - 先执行：`mvn dependency:copy-dependencies`

2. **PowerShell 报命令找不到（如 `head`）**
    - 使用 `Select-Object -First N` 替代

3. **想进一步加快实验**
    - 维持 `3-10ms` 默认延迟，或手动传更小范围（例如 `2 6`）
