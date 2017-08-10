package org.cripac.isee.alg.pedestrian.attr;

import java.util.List;

public class Hour {

	private Long hour;
	private List<Minute> HAS_MINList;
	public Long getHour() {
		return hour;
	}
	public void setHour(Long hour) {
		this.hour = hour;
	}
	public List<Minute> getHAS_MINList() {
		return HAS_MINList;
	}
	public void setHAS_MINList(List<Minute> hAS_MINList) {
		HAS_MINList = hAS_MINList;
	}
	@Override
	public String toString() {
		return "Hour [hour=" + hour + ", HAS_MINList=" + HAS_MINList + "]";
	}
	
	
}
