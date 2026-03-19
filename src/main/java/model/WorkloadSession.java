package model;

import java.util.List;

public class WorkloadSession {
	private int id;
	private List<ProgramInstance> transactions;

	public WorkloadSession() {
		this.id = 0;
		this.transactions = null;
	}

	public WorkloadSession(int id, List<ProgramInstance> transactions) {
		this.id = id;
		this.transactions = transactions;
	}

	public int getId() {
		return id;
	}

	public List<ProgramInstance> getTransactions() {
		return transactions;
	}
}
