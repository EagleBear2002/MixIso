package model;

public class Edge {
	public ProgramInstance from;
	public ProgramInstance to;
	public String variable;

	public Edge(ProgramInstance from, ProgramInstance to, String variable) {
		this.from = from;
		this.to = to;
		this.variable = variable;
	}
}