package org.cripac.isee.alg.pedestrian.attr;

import java.util.List;

public class Day {
	
	private Long day;
	private List<Hour> HAS_HOURList;
	public Long getDay() {
		return day;
	}
	public void setDay(Long day) {
		this.day = day;
	}
	public List<Hour> getHAS_HOURList() {
		return HAS_HOURList;
	}
	public void setHAS_HOURList(List<Hour> hAS_HOURList) {
		HAS_HOURList = hAS_HOURList;
	}
	
	

}
