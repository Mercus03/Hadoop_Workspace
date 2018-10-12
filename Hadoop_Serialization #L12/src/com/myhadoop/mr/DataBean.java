/**
 * 84116
 *2018年10月10日
 */
package com.myhadoop.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * @author 84116
 *
 */
public class DataBean implements Writable{
    
    //手机号 
    private String telNo;
    //上行流量
    private long upPayLoad;
    
    private long downPayLoad;
    
    private long totalPayLoad;
    
    
    /* (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        // TODO Auto-generated method stub
        this.telNo=in.readUTF();
        this.upPayLoad=in.readLong();
        this.downPayLoad=in.readLong();
        this.totalPayLoad=in.readLong();
        
    }

    
    /* (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput arg0) throws IOException {
        // TODO Auto-generated method stub
        arg0.writeUTF(telNo);
        arg0.writeLong(downPayLoad);
        arg0.writeLong(totalPayLoad);
    }
    
    
    public DataBean() {
        
    }
    
    public DataBean(String telNo,long upPayLoad,long downPayLoad,long totalPayLoad) {
        super();
        this.telNo=telNo;
        this.upPayLoad=upPayLoad;
        this.downPayLoad=downPayLoad;
        this.totalPayLoad=upPayLoad+downPayLoad;
    }
    
    @Override
    public String toString() {
        return this.upPayLoad+"\t"+this.downPayLoad+"\t"+this.totalPayLoad;
    }


    public String getTelNo() {
        return telNo;
    }


    public void setTelNo(String telNo) {
        this.telNo = telNo;
    }


    public long getUpPayLoad() {
        return upPayLoad;
    }


    public void setUpPayLoad(long upPayLoad) {
        this.upPayLoad = upPayLoad;
    }


    public long getDownPayLoad() {
        return downPayLoad;
    }


    public void setDownPayLoad(long downPayLoad) {
        this.downPayLoad = downPayLoad;
    }


    public long getTotalPayLoad() {
        return totalPayLoad;
    }


    public void setTotalPayLoad(long totalPayLoad) {
        this.totalPayLoad = totalPayLoad;
    }
    


}
