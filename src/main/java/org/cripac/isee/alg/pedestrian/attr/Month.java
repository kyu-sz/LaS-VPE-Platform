package org.cripac.isee.alg.pedestrian.attr;

import java.util.List;

public class Month {

	private Long month;
	private List<Day> dayList;
	public Long getMonth() {
		return month;
	}
	public void setMonth(Long month) {
		this.month = month;
	}
	public List<Day> getDayList() {
		return dayList;
	}
	public void setDayList(List<Day> dayList) {
		this.dayList = dayList;
	}
	@Override
	public String toString() {
		return "Month [month=" + month + ", dayList=" + dayList + "]";
	}
	
}
