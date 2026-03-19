package algorithm;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.imageio.ImageIO;

public class VisualizationExporter {
	public static void generateBenchDistributionPng(Path distributionCsv, Path outputPng) throws Exception {
		if (!Files.exists(distributionCsv)) {
			return;
		}

		List<String[]> rows = new ArrayList<>();
		try (BufferedReader reader = new BufferedReader(new FileReader(distributionCsv.toFile()))) {
			String line = reader.readLine();
			while ((line = reader.readLine()) != null) {
				String[] parts = line.split(",");
				if (parts.length >= 6) {
					rows.add(parts);
				}
			}
		}

		if (rows.isEmpty()) {
			return;
		}

		rows.sort(Comparator.comparing(a -> a[0]));

		int width = Math.max(1400, 60 * rows.size() + 180);
		int height = 700;
		int left = 70;
		int top = 60;
		int chartW = width - left - 40;
		int chartH = height - top - 120;

		BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
		Graphics2D g = image.createGraphics();
		g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
		g.setColor(Color.WHITE);
		g.fillRect(0, 0, width, height);

		g.setColor(Color.BLACK);
		g.setFont(new Font("SansSerif", Font.BOLD, 20));
		g.drawString("Benchmark Isolation Allocation Distribution", left, 35);

		Color[] colors = new Color[]{Color.RED, Color.ORANGE, Color.YELLOW, Color.CYAN, Color.GREEN};
		String[] labels = new String[]{"SER", "SI", "PSI", "PC", "RA"};

		g.setColor(Color.LIGHT_GRAY);
		for (int y = 0; y <= 10; y++) {
			int py = top + chartH - y * chartH / 10;
			g.drawLine(left, py, left + chartW, py);
		}

		int barW = Math.max(8, chartW / rows.size() - 6);
		for (int i = 0; i < rows.size(); i++) {
			String[] row = rows.get(i);
			double[] vals = new double[]{
					Double.parseDouble(row[1]),
					Double.parseDouble(row[2]),
					Double.parseDouble(row[3]),
					Double.parseDouble(row[4]),
					Double.parseDouble(row[5])
			};

			int x = left + i * (barW + 6);
			int bottom = top + chartH;
			for (int k = 0; k < vals.length; k++) {
				int h = (int) Math.round((vals[k] / 100.0) * chartH);
				g.setColor(colors[k]);
				g.fillRect(x, bottom - h, barW, h);
				bottom -= h;
			}

			if (i % Math.max(1, rows.size() / 20) == 0) {
				g.setColor(Color.DARK_GRAY);
				g.setFont(new Font("SansSerif", Font.PLAIN, 9));
				String shortName = row[0].replace(".json", "");
				if (shortName.length() > 14) {
					shortName = shortName.substring(0, 14);
				}
				g.drawString(shortName, x, top + chartH + 14);
			}
		}

		g.setColor(Color.BLACK);
		g.drawRect(left, top, chartW, chartH);
		for (int y = 0; y <= 10; y++) {
			int val = y * 10;
			int py = top + chartH - y * chartH / 10;
			g.drawString(String.valueOf(val), 30, py + 4);
		}

		int lx = left;
		int ly = height - 45;
		for (int i = 0; i < labels.length; i++) {
			g.setColor(colors[i]);
			g.fillRect(lx, ly - 10, 16, 10);
			g.setColor(Color.BLACK);
			g.drawString(labels[i], lx + 22, ly);
			lx += 90;
		}

		if (outputPng.getParent() != null) {
			Files.createDirectories(outputPng.getParent());
		}
		ImageIO.write(image, "png", outputPng.toFile());
		g.dispose();
	}

	public static void generateRandomAnalysisAndPng(Path perfCsv, Path analysisCsv, Path outputPng) throws Exception {
		if (!Files.exists(perfCsv)) {
			return;
		}

		List<Record> records = new ArrayList<>();
		Pattern p = Pattern.compile("workload_(\\d+)t_(\\d+)o_(\\d+)k_(\\d+)r_(\\d+)\\.json");
		try (BufferedReader reader = new BufferedReader(new FileReader(perfCsv.toFile()))) {
			String line = reader.readLine();
			while ((line = reader.readLine()) != null) {
				String[] cols = line.split(",", 4);
				if (cols.length < 3 || !"success".equals(cols[1])) {
					continue;
				}
				Matcher m = p.matcher(cols[0]);
				if (!m.matches()) {
					continue;
				}
				records.add(new Record(
						Integer.parseInt(m.group(1)),
						Integer.parseInt(m.group(2)),
						Integer.parseInt(m.group(3)),
						Integer.parseInt(m.group(4)),
						Double.parseDouble(cols[2])
				));
			}
		}

		if (records.isEmpty()) {
			return;
		}

		Map<Integer, Stats> byTxns = new HashMap<>();
		Map<Integer, Stats> byOps = new HashMap<>();
		Map<Integer, Stats> byKey = new HashMap<>();
		for (Record r : records) {
			byTxns.computeIfAbsent(r.txns, k -> new Stats()).add(r.sec);
			byOps.computeIfAbsent(r.maxOps, k -> new Stats()).add(r.sec);
			byKey.computeIfAbsent(r.maxKey, k -> new Stats()).add(r.sec);
		}

		if (analysisCsv.getParent() != null) {
			Files.createDirectories(analysisCsv.getParent());
		}
		try (PrintWriter writer = new PrintWriter(new FileWriter(analysisCsv.toFile()))) {
			writer.println("vary_param,vary_value,mean,std,count");
			writeStats(writer, "txns", byTxns);
			writeStats(writer, "max_ops", byOps);
			writeStats(writer, "max_key", byKey);
		}

		drawThreePanel(outputPng, byTxns, byOps, byKey);
	}

	private static void writeStats(PrintWriter writer, String param, Map<Integer, Stats> map) {
		map.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(e -> {
			Stats s = e.getValue();
			writer.printf("%s,%d,%.6f,%.6f,%d%n", param, e.getKey(), s.mean(), s.std(), s.count);
		});
	}

	private static void drawThreePanel(Path outputPng,
								 Map<Integer, Stats> byTxns,
								 Map<Integer, Stats> byOps,
								 Map<Integer, Stats> byKey) throws Exception {
		int width = 1500;
		int height = 520;
		BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
		Graphics2D g = image.createGraphics();
		g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
		g.setColor(Color.WHITE);
		g.fillRect(0, 0, width, height);
		g.setColor(Color.BLACK);
		g.setFont(new Font("SansSerif", Font.BOLD, 18));
		g.drawString("Allocation Performance vs Workload Parameters", 30, 30);

		drawPanel(g, 20, 60, 460, 420, "txns", byTxns);
		drawPanel(g, 510, 60, 460, 420, "max_ops", byOps);
		drawPanel(g, 1000, 60, 460, 420, "max_key", byKey);

		if (outputPng.getParent() != null) {
			Files.createDirectories(outputPng.getParent());
		}
		ImageIO.write(image, "png", outputPng.toFile());
		g.dispose();
	}

	private static void drawPanel(Graphics2D g, int x, int y, int w, int h, String title, Map<Integer, Stats> map) {
		g.setColor(Color.BLACK);
		g.setFont(new Font("SansSerif", Font.BOLD, 14));
		g.drawString(title, x + 8, y - 8);
		g.drawRect(x, y, w, h);

		if (map.isEmpty()) {
			return;
		}

		List<Integer> xs = new ArrayList<>(map.keySet());
		xs.sort(Integer::compareTo);

		double max = 0;
		for (Integer k : xs) {
			max = Math.max(max, map.get(k).mean() + map.get(k).std());
		}
		if (max <= 0) {
			max = 1;
		}

		int n = xs.size();
		int gap = Math.max(12, (w - 40) / Math.max(1, n));
		int barW = Math.max(10, gap - 8);
		int startX = x + 24;
		int baseY = y + h - 24;

		for (int i = 0; i < n; i++) {
			Integer key = xs.get(i);
			Stats s = map.get(key);
			int bh = (int) Math.round((s.mean() / max) * (h - 60));
			int bx = startX + i * gap;
			int by = baseY - bh;

			g.setColor(new Color(70, 130, 180));
			g.fillRect(bx, by, barW, bh);

			int err = (int) Math.round((s.std() / max) * (h - 60));
			g.setColor(Color.BLACK);
			g.setStroke(new BasicStroke(1.5f));
			int cx = bx + barW / 2;
			g.drawLine(cx, by - err, cx, by + err);
			g.drawLine(cx - 5, by - err, cx + 5, by - err);
			g.drawLine(cx - 5, by + err, cx + 5, by + err);

			g.setFont(new Font("SansSerif", Font.PLAIN, 10));
			String label = String.valueOf(key);
			if ("max_key".equals(title) && key >= 1000) {
				label = (key / 1000) + "k";
			}
			g.drawString(label, bx, baseY + 14);
		}

		g.setFont(new Font("SansSerif", Font.PLAIN, 10));
		g.drawString("sec", x + 4, y + 12);
	}

	private static class Record {
		final int txns;
		final int maxOps;
		final int maxKey;
		final int readOnly;
		final double sec;

		Record(int txns, int maxOps, int maxKey, int readOnly, double sec) {
			this.txns = txns;
			this.maxOps = maxOps;
			this.maxKey = maxKey;
			this.readOnly = readOnly;
			this.sec = sec;
		}
	}

	private static class Stats {
		int count = 0;
		double sum = 0.0;
		double sumSq = 0.0;

		void add(double x) {
			count++;
			sum += x;
			sumSq += x * x;
		}

		double mean() {
			return count == 0 ? 0.0 : sum / count;
		}

		double std() {
			if (count <= 1) {
				return 0.0;
			}
			double m = mean();
			double var = Math.max(0.0, sumSq / count - m * m);
			return Math.sqrt(var);
		}
	}
}
