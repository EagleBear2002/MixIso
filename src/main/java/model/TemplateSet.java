package model;

import java.util.List;

// 添加Jackson ObjectMapper依赖


public class TemplateSet {
	private List<ProgramInstance> templates;

	public TemplateSet(List<ProgramInstance> templates) {
		this.templates = templates;
	}

	public TemplateSet() {
		this.templates = null;
	}

	public List<ProgramInstance> getTemplates() {
		return templates;
	}

	public String toString() {
		return super.toString();
	}
}