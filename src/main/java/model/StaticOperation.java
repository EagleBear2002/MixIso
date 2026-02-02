package model;

public class StaticOperation {
	private final int id;
	private final OperationType type;
	private final String key;

	public StaticOperation() {
		this.id = -1;
		this.type = null;
		this.key = null;
	}

	public StaticOperation(int id, OperationType type, String key, int value) {
		this.id = id;
		this.type = type;
		this.key = key;
	}

	public static StaticOperation read(int id, String key) {
		return new StaticOperation(id, OperationType.READ, key, -1);
	}

	public static StaticOperation write(int id, String key, int value) {
		return new StaticOperation(id, OperationType.WRITE, key, value);
	}

	// Getters
	public int getId() { return id; }
	public OperationType getType() { return type; }
	public String getKey() { return key; }
	public boolean isWriteOp() { return OperationType.WRITE.equals(type) || OperationType.UPDATE.equals(type); }
	public boolean isReadOp() { return OperationType.READ.equals(type) || OperationType.UPDATE.equals(type); }
	public String toString() {
		return "{\"id\": " + id + ", \"type\": \"" + type + "\", \"key\": " + key + "}";
	}
}