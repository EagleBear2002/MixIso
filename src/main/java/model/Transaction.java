package model;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Transaction {
	private final int id;
	private final IsolationLevel isolationLevel;
	private final List<Operation> operations;
	private int startTimeStamp;
	private long realStartTimeMs;  // 添加实际开始时间（毫秒）
	private int commitTimeStamp;
	private long realCommitTimeMs;  // 添加实际提交时间（毫秒）
	
	public Transaction() {
		this.id = -1;
		this.isolationLevel = null;
		this.operations = null;
		startTimeStamp = -1;
		commitTimeStamp = -1;
	}
	
	public Transaction(int id, IsolationLevel isolationLevel, List<Operation> operations) {
		this.id = id;
		this.isolationLevel = isolationLevel;
		this.operations = operations;
		startTimeStamp = -1;
		commitTimeStamp = -1;
	}
	
	public boolean isSER() {
		return isolationLevel.equals(IsolationLevel.SERIALIZABLE);
	}
	
	public boolean isSI() {
		return isolationLevel.equals(IsolationLevel.SNAPSHOT_ISOLATION);
	}
	
	public boolean isPC() {
		return isolationLevel.equals(IsolationLevel.PREFIX_CONSISTENCY);
	}
	
	public boolean isPSI() {
		return isolationLevel.equals(IsolationLevel.PARALLEL_SNAPSHOT_ISOLATION);
	}
	
	public boolean isReadOnly() {
		return getWriteSet().isEmpty();
	}
	
	public Set<Integer> getWriteSet() {
		Set<Integer> writeSet = new HashSet<>();
		if (operations != null) {
			for (Operation op : operations) {
				if (op.isWriteOp()) {
					writeSet.add(op.getKey());
				}
			}
		}
		return writeSet;
	}
	
	public IsolationLevel getIsolationLevel() {
		return isolationLevel;
	}
	
	public List<Operation> getOperations() {
		return operations;
	}
	
	public void setBothStartTime(int startTimeStamp) {
		this.startTimeStamp = startTimeStamp;
		this.realStartTimeMs = System.currentTimeMillis();  // 记录实际开始时间
	}
	
	public int getStartTimeStamp() {
		return startTimeStamp;
	}
	
	public void setBothCommitTime(int commitTimeStamp) {
		this.commitTimeStamp = commitTimeStamp;
		this.realCommitTimeMs = System.currentTimeMillis();  // 记录实际提交时间
	}
	
	public void setRealCommitTimeMs(long realCommitTimeMs) {
		this.realCommitTimeMs = realCommitTimeMs;
	}
	
	public int getCommitTimeStamp() {
		return commitTimeStamp;
	}
	
	public boolean wwConflict(Transaction txn) {
		for (Operation op1 : operations) {
			for (Operation op2 : txn.operations) {
				if (op1.getKey() == op2.getKey() && op1.isWriteOp() && op2.isWriteOp()) {
					return true;
				}
			}
		}
		return false;
	}
	
	public boolean wrConflict(Transaction txn) {
		for (Operation op1 : operations) {
			for (Operation op2 : txn.operations) {
				if (op1.getKey() == op2.getKey() && op1.isWriteOp() && !op2.isWriteOp()) {
					return true;
				}
			}
		}
		return false;
	}
	
	public int getId() {
		return id;
	}
	
	public String toString() {
		return "{\"id\": " + id + ", \"isolationLevel\": \"" + isolationLevel + "\", \"operations\": " + operations + "}";
	}
	
	public boolean isCC() {
		return isolationLevel.equals(IsolationLevel.CAUSAL_CONSISTENCY);
	}
	
	public boolean isRA() {
		return isolationLevel.equals(IsolationLevel.READ_ATOMIC);
	}
	
	public long getRealStartTimeMs() {
		return realStartTimeMs;
	}
	
	public long getRealCommitTimeMs() {
		return realCommitTimeMs;
	}
}