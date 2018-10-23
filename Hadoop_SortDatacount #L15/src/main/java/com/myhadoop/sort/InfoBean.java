package com.myhadoop.sort;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/*
 * MapReduce是以<key,value>形式存放数据的
 * 数据比较多时，需要用类进行封装
 * 自定义排序重点在于compareTo方法
 */

public class InfoBean implements WritableComparable<InfoBean> {
	private String account;//帐号
	private double income;//收入，这里我们不考虑多么精准，因我我们用到的数据都是整数
	private double outcome;//支出
	private double surplus;//结余

	public void set(String account,double income,double outcome){
	this.account=account;
	this.income=income;
	this.outcome=outcome;
	this.surplus=income-outcome;
	}
	/**
	* 序列化Bean
	*/
	public void write(DataOutput out) throws IOException {
		out.writeUTF(account);
		out.writeDouble(income);
		out.writeDouble(outcome);
		out.writeDouble(surplus);
	}
	
	
	/*
	    *实现的功能:
	    *按照用户的收入金额排序收入金额越大排序越靠前，
	    *如果收入金额一样则比较支出，支出越少排名越靠前。
	 */

	/**
	* 反序列化Bean
	*/
	public void readFields(DataInput in) throws IOException {
		this.account=in.readUTF();
		this.income=in.readDouble();
		this.outcome=in.readDouble();
		this.surplus=in.readDouble();
	}
	/**
     * 自定义比较方法，返回到int值是参数Bean与当前Bean的比较结果
     * 如果传过来到Bean的收入值小于当前Bean的收入值，那么应该返回-1;
     * 表示传过来的Bean应该排在当前Bean的后面。
     * 如果传过来的Bean与当前Bean的收入值一样，那么就再比较支出的值，
     * 如果传过来的Bean的支出值小于当前Bean的支出值的话，应该返回1，
     * 表示传过来的Bean应该排在当前Bean的前面。
     */
	public int compareTo(InfoBean o) {
		if(this.income==o.getIncome()){
			return this.getOutcome()>o.getOutcome() ? 1:-1;
		}else{
			return this.getIncome()>o.getIncome() ? -1:1;
		}
	}

	/**
  * 重写toString方法
  */
	@Override
	public String toString() {
		return this.income+"\t"+this.outcome+"\t"+this.surplus;
	}
	
	public String getAccount() {
		return account;
	}
	public void setAccount(String account) {
		this.account = account;
	}
	public double getIncome() {
		return income;
	}
	public void setIncome(double income) {
		this.income = income;
	}
	public double getOutcome() {
		return outcome;
	}
	public void setOutcome(double outcome) {
		this.outcome = outcome;
	}
	public double getSurplus() {
		return surplus;
	}
	public void setSurplus(double surplus) {
		this.surplus = surplus;
	}
	
}
