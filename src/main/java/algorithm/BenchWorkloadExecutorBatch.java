package algorithm;

import model.ExecutionResult;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class BenchWorkloadExecutorBatch {
	public static void main(String[] args) throws Exception {
		int dcCount = args.length >= 1 ? Integer.parseInt(args[0]) : 5;
		int minRtt = args.length >= 2 ? Integer.parseInt(args[1]) : 100;
		int maxRtt = args.length >= 3 ? Integer.parseInt(args[2]) : 300;
		String outputCsv = args.length >= 4 ? args[3] : "data/bench_execution_results.csv";

		Path projectDir = Paths.get(".").toAbsolutePath().normalize();
		Path allocatedDir = projectDir.resolve("data").resolve("allocated_bench_workload");
		Path csvPath = projectDir.resolve(outputCsv);
		if (csvPath.getParent() != null) {
			Files.createDirectories(csvPath.getParent());
		}

		List<Path> files = new ArrayList<>();
		Files.list(allocatedDir)
				.filter(p -> p.getFileName().toString().endsWith(".json"))
				.sorted(Comparator.comparing(p -> p.getFileName().toString()))
				.forEach(files::add);

		if (files.isEmpty()) {
			System.out.println("No allocated benchmark files found in " + allocatedDir);
			return;
		}

		int ok = 0;
		int fail = 0;
		for (Path file : files) {
			try {
				ExecutionResult result = Executor.runSingleWorkload(file.toString(), csvPath.toString(), dcCount, minRtt, maxRtt);
				ok++;
				System.out.printf("[OK] %s committed=%d aborted=%d throughput=%.2f%n",
						file.getFileName(),
						result.getCommittedTxns(),
						result.getAbortedTxns(),
						result.getThroughputTxPerSec());
			} catch (Exception e) {
				fail++;
				System.out.println("[FAIL] " + file.getFileName() + " => " + e.getMessage());
			}
		}

		System.out.println("Execution finished. success=" + ok + " failed=" + fail + " output=" + csvPath);
	}
}
