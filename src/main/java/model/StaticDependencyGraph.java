package model;

import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Queue;
import java.util.LinkedList;
import java.util.Collections;

public class StaticDependencyGraph {
	private TemplateSet templateSet;
	private List<Edge> WREdges;
	private List<Edge> WWEdges;
	private List<Edge> RWEdges;

	public void insertWR(ProgramInstance t1, ProgramInstance t2, String var) {
		WREdges.add(new Edge(t1, t2, var));
	}

	public void insertWW(ProgramInstance t1, ProgramInstance t2, String var) {
		WWEdges.add(new Edge(t1, t2, var));
	}

	public void insertRW(ProgramInstance t1, ProgramInstance t2, String var) {
		RWEdges.add(new Edge(t1, t2, var));
	}

	// Check if there is a path from t1 to t2, including direct edges and transitive paths
	public boolean hasPathWithoutReadOnly(ProgramInstance t1, ProgramInstance t2) {
//		if (t1.isReadOnly() || t2.isReadOnly()) {
//			return false;
//		}

		if (t1 == t2) {
			return true;
		}

		Set<ProgramInstance> visited = new HashSet<>();
		Queue<ProgramInstance> queue = new LinkedList<>();
		queue.add(t1);
		visited.add(t1);

		while (!queue.isEmpty()) {
			ProgramInstance u = queue.poll();
//			if (u.isReadOnly()) {
//				continue;
//			}

			List<Edge> allEdges = WREdges;
			allEdges.addAll(WWEdges);
			allEdges.addAll(RWEdges);

			for (Edge edge : allEdges) {
				if (edge.from == u && !visited.contains(edge.to)) {
					if (edge.to == t2) {
						return true;
					}
					visited.add(edge.to);
					queue.add(edge.to);
				}
			}
		}

		return false;
	}

	public StaticDependencyGraph(TemplateSet templateSet) {
		this.templateSet = templateSet;
		this.WREdges = new ArrayList<>();
		this.RWEdges = new ArrayList<>();
		this.WWEdges = new ArrayList<>();

		// Insert edges based on template set
		List<ProgramInstance> templates = templateSet.getTemplates();
		for (int i = 0; i < templates.size(); i++) {
			for (int j = 0; j < templates.size(); j++) {
				if (i != j) {
					ProgramInstance t1 = templates.get(i);
					ProgramInstance t2 = templates.get(j);

					// Check for WR edges
					for (String var : t1.getWriteSet()) {
						if (t2.getReadSet().contains(var)) {
//							System.out.println("WR edge: " + t1.getName() + " -> " + t2.getName() + " (" + var + ")");
							insertWR(t1, t2, var);
						}
					}

					// Check for WW edges
					for (String var : t1.getWriteSet()) {
						if (t2.getWriteSet().contains(var)) {
//							System.out.println("WW edge: " + t1.getName() + " -> " + t2.getName() + " (" + var + ")");
							insertWW(t1, t2, var);
						}
					}

					// Check for RW edges
					for (String var : t1.getReadSet()) {
						if (t2.getWriteSet().contains(var)) {
//							System.out.println("RW edge: " + t1.getName() + " -> " + t2.getName() + " (" + var + ")");
							insertRW(t1, t2, var);
						}
					}
				}
			}
		}
	}

	public List<Edge> getWREdges() {
		return WREdges;
	}

	public List<Edge> getRWEdges() {
		return RWEdges;
	}

	public List<Edge> getWWEdges() {
		return WWEdges;
	}

	public boolean hasCriticalCycle() {
		for (Edge rwEdge23 : RWEdges) {
			ProgramInstance p2 = rwEdge23.from;
			ProgramInstance p3 = rwEdge23.to;

			if (p2.isSingleRead() || p2.isWriteOnly()) {
				continue;
			}

			if (p2.isSI()) {
				if (!Collections.disjoint(p2.getWriteSet(), p3.getWriteSet())) {
					continue;
				}

				for (Edge rwEdge12 : RWEdges) {
					ProgramInstance p1 = rwEdge12.from;
					if (rwEdge12.to.equals(p2)
//							&& !p1.isReadOnly()
							&& !rwEdge12.variable.equals(rwEdge23.variable)
							&& Collections.disjoint(p1.getWriteSet(), p2.getWriteSet())
							&& hasPathWithoutReadOnly(p3, p1)) {
						System.out.println("Critical cycle 1 found: " + p1.getName() + " -> " + p2.getName() + " -> " + p3.getName());
						return true;
					}
				}
			} else if (p2.isPC()) {
				ArrayList<Edge> RW_WWEdges = new ArrayList<>();
				RW_WWEdges.addAll(WWEdges);
				RW_WWEdges.addAll(RWEdges);

				for (Edge edge12 : RW_WWEdges) {
					ProgramInstance p1 = edge12.from;
					if (edge12.to.equals(p2)
//							&& !p1.isReadOnly()
							&& !edge12.variable.equals(rwEdge23.variable)
							&& hasPathWithoutReadOnly(p3, p1)) {
						System.out.println("Critical cycle 2 found: " + p1.getName() + " -> " + p2.getName() + " -> " + p3.getName());
						return true;
					}
				}
			} else if (p2.isPSI()) {
				if (!Collections.disjoint(p2.getWriteSet(), p3.getWriteSet())) {
					continue;
				}

				for (ProgramInstance p1 : templateSet.getTemplates()) {
					if (hasDirectEdgeTo(p1, p2)
//							&& !p1.isReadOnly()
							&& hasPathWithoutReadOnly(p3, p1)) {
						System.out.println("Critical cycle 3 found: " + p1.getName() + " -> " + p2.getName() + " -> " + p3.getName());
						return true;
					}
				}
			} else if (p2.isCC() || p2.isRA()) {
				for (ProgramInstance p1 : templateSet.getTemplates()) {
					if (hasDirectEdgeTo(p1, p2)
//							&& !p1.isReadOnly()
							&& hasPathWithoutReadOnly(p3, p1)) {
						System.out.println("Critical cycle 4 found: " + p1.getName() + " -> " + p2.getName() + " -> " + p3.getName());
						return true;
					}
				}
			}
		}

		return false;
	}

	private boolean hasDirectEdgeTo(ProgramInstance from, ProgramInstance to) {
		List<Edge> allEdges = WREdges;
		allEdges.addAll(WWEdges);
		allEdges.addAll(RWEdges);

		for (Edge edge : allEdges) {
			if (edge.from == from && edge.to == to) {
				return true;
			}
		}

		return false;
	}
}