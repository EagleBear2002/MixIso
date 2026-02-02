# Artifact and Technical Report for On Mixing Database Isolation Levels

Technical Report: [`tech-rpt.pdf`](https://github.com/EagleBear2002/MixIso/blob/main/tech-rpt.pdf).

## Directory Organization

```plain
TODO: 
/MixIso
|-- tech-rpt.pdf            # the accompanying technical report
|-- README.md
|-- reproduce               # scripts of reproducing experiments
|-- precompiled             # precompiled instances of VeriStrong under Ubuntu 22.04, used for reproducing experiments
`-- tools       
    ├── CobraVerifier       # The Cobra Checker
    ├── PolySI              # The PolySI Checker (modified)
    ├── Viper               # The Viper Checker
    └── dbcop-verifier      # The dbcop Checker
```

### Q1

```
python3 scripts/generate_bench_workload.py --txns 100 --max-key 500 --cases 10
```

```
python3 scripts/allocate_bench_workload.py
```

### Q2

```
python3 scripts/generate_random_workload.py --txns 500 --max-ops 10 --max-key 500 --read-only 50 --cases 5
```

```
python3 scripts/random_workload_for_test.py
```

```
python3 scripts/allocate_random_workload.py
```
