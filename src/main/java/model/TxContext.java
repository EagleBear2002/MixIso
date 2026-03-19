package model;

import java.util.HashMap;
import java.util.Map;

public class TxContext {
	private final IsolationLevel level;
	private final long sts;
	private final Map<String, Integer> buffer;
	private int lastReadValue;

	public TxContext(IsolationLevel level, long sts) {
		this.level = level;
		this.sts = sts;
		this.buffer = new HashMap<>();
		this.lastReadValue = 0;
	}

	public IsolationLevel getLevel() {
		return level;
	}

	public long getSts() {
		return sts;
	}

	public Map<String, Integer> getBuffer() {
		return buffer;
	}

	public int getLastReadValue() {
		return lastReadValue;
	}

	public void setLastReadValue(int lastReadValue) {
		this.lastReadValue = lastReadValue;
	}
}