package model;

import java.util.List;

public class Session {
    private final int id;
    private final List<Transaction> transactions;

    // 确保无参构造函数存在
    public Session() {
        this.id = 0;
        this.transactions = null;
    }

    // 确保有参构造函数存在
    public Session(int id, List<Transaction> transactions) {
        this.id = id;
        this.transactions = transactions;
    }

    public List<Transaction> getTransactions() {
        return transactions;
    }

	public int getId() {
		return id;
	}

    public String toString() {
        return "{\"id\":" + id + ",\"transactions\":" + transactions + "}";
    }
}