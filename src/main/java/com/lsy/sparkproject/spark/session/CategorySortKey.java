package com.lsy.sparkproject.spark.session;


import scala.Serializable;
import scala.math.Ordered;

/**
 * 品类二次排序key
 * 
 * 封装要进行排序算法需要的几个字段：点击次数、下单次数和支付次数
 * 实现Ordered接口要求的几个方法
 * 
 * 注意：必须实现Se接口表明可以序列化
 * @author work
 *
 */
public class CategorySortKey  implements Ordered<CategorySortKey>,Serializable{
	
	private static final long serialVersionUID = 1L;
	private long clickCount;
	private long orderCount;
	private long payCount;
	
	@Override
	public boolean $greater(CategorySortKey other) {
		if(clickCount > other.getClickCount()){
			return true;
		}else if(clickCount == other.getClickCount() && orderCount > other.getOrderCount()){
			return true;
		}else if (clickCount == other.getClickCount() && orderCount == other.getOrderCount() && payCount > other.getPayCount()){
			return true;
		}
		return false;
	}

	@Override
	public boolean $greater$eq(CategorySortKey other) {
		if($greater(other)){
			return true;
		}else if  (clickCount == other.getClickCount() && orderCount == other.getOrderCount() && payCount == other.getPayCount()){
			return true;
		}
		return false;
	}

	@Override
	public boolean $less(CategorySortKey other) {
		if(clickCount < other.getClickCount()){
			return true;
		}else if(clickCount == other.getClickCount() && orderCount < other.getOrderCount()){
			return true;
		}else if (clickCount == other.getClickCount() && orderCount == other.getOrderCount() && payCount < other.getPayCount()){
			return true;
		}
		return false;
	}

	@Override
	public boolean $less$eq(CategorySortKey other) {
		if($less(other)){
			return true;
		}else if  (clickCount == other.getClickCount() && orderCount == other.getOrderCount() && payCount == other.getPayCount()){
			return true;
		}
		return false;
	}

	@Override
	public int compare(CategorySortKey other) {
		if(clickCount - other.clickCount != 0){
			return (int) (clickCount - other.clickCount);
		}else if(orderCount - other.orderCount != 0){
			return (int) (orderCount - other.orderCount);
		}else if (payCount - other.payCount != 0){
			return (int) (payCount - other.payCount);
		}
		return 0;
	}

	@Override
	public int compareTo(CategorySortKey other) {
		if(clickCount - other.clickCount != 0){
			return (int) (clickCount - other.clickCount);
		}else if(orderCount - other.orderCount != 0){
			return (int) (orderCount - other.orderCount);
		}else if (payCount - other.payCount != 0){
			return (int) (payCount - other.payCount);
		}
		return 0;
	}

	public long getClickCount() {
		return clickCount;
	}

	public void setClickCount(long clickCount) {
		this.clickCount = clickCount;
	}

	public long getOrderCount() {
		return orderCount;
	}

	public void setOrderCount(long orderCount) {
		this.orderCount = orderCount;
	}

	public long getPayCount() {
		return payCount;
	}

	public void setPayCount(long payCount) {
		this.payCount = payCount;
	}

	public CategorySortKey(long clickCount, long orderCount, long payCount) {
		this.clickCount = clickCount;
		this.orderCount = orderCount;
		this.payCount = payCount;
	}
	

}
