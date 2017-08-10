package org.cripac.isee.alg.pedestrian.attr;

import java.util.List;

public class Minute {
	
	private Long start;
	private Long end;
	private List<ReIdAttributesTemp> INCLUDES_PERSONList;
	public Long getStart() {
		return start;
	}

	public void setStart(Long start) {
		this.start = start;
	}

	public Long getEnd() {
		return end;
	}

	public void setEnd(Long end) {
		this.end = end;
	}

	public List<ReIdAttributesTemp> getINCLUDES_PERSONList() {
		return INCLUDES_PERSONList;
	}

	public void setINCLUDES_PERSONList(List<ReIdAttributesTemp> iNCLUDES_PERSONList) {
		INCLUDES_PERSONList = iNCLUDES_PERSONList;
	}

	@Override
	public String toString() {
		return "Minute [start=" + start + ", end=" + end + ", INCLUDES_PERSONList=" + INCLUDES_PERSONList + "]";
	}

	

}
