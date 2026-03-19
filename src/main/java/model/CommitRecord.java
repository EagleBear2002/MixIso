package model;

import java.util.HashSet;
import java.util.Set;

public class CommitRecord {
	private final long sts;
	private final long cts;
	private final Set<String> readSet;
	private final Set<String> writeSet;

	public CommitRecord(long sts, long cts, Set<String> readSet, Set<String> writeSet) {
		this.sts = sts;
		this.cts = cts;
		this.readSet = new HashSet<>(readSet);
		this.writeSet = new HashSet<>(writeSet);
	}

	public long getSts() {
		return sts;
	}

	public long getCts() {
		return cts;
	}

	public Set<String> getReadSet() {
		return readSet;
	}

	public Set<String> getWriteSet() {
		return writeSet;
	}
}