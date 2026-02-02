package model;

public class Operation {
	private final int id;
	private final OperationType type;
	private final int key;
	private final int value;

	public Operation() {
		this.id = -1;
		this.type = null;
		this.key = -1;
		this.value = -1;
	}

	public Operation(int id, OperationType type, int key, int value) {
		this.id = id;
		this.type = type;
		this.key = key;
		this.value = value;
	}

	public static Operation read(int id, int key) {
		return new Operation(id, OperationType.READ, key, -1);
	}

	public static Operation write(int id, int key, int value) {
		return new Operation(id, OperationType.WRITE, key, value);
	}

	// Getters
	public int getId() { return id; }
	public OperationType getType() { return type; }
	public int getKey() { return key; }
	public Integer getValue() { return value; }
	public boolean isWriteOp() { return OperationType.WRITE.equals(type); }
	public boolean isReadOp() { return OperationType.READ.equals(type); }
	public String toString() {
		return "{\"id\": " + id + ", \"type\": \"" + type + "\", \"key\": " + key + ", \"value\": " + value + "}";
	}
}