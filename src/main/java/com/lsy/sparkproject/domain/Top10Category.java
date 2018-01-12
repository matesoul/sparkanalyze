package com.lsy.sparkproject.domain;

public class Top10Category {
	private long taskid;
	private long categoryid;
	private long clickcount;
	private long ordercount;
	private long paycount;
	public long getTaskid() {
		return taskid;
	}
	public void setTaskid(long taskid) {
		this.taskid = taskid;
	}
	public long getCategoryid() {
		return categoryid;
	}
	public void setCategoryid(long categoryid) {
		this.categoryid = categoryid;
	}
	public long getClickcount() {
		return clickcount;
	}
	public void setClickcount(long clickcount) {
		this.clickcount = clickcount;
	}
	public long getOrdercount() {
		return ordercount;
	}
	public void setOrdercount(long ordercount) {
		this.ordercount = ordercount;
	}
	public long getPaycount() {
		return paycount;
	}
	public void setPaycount(long paycount) {
		this.paycount = paycount;
	}
	
}
