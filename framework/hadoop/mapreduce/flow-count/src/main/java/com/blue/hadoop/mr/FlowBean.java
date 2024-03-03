package com.blue.hadoop.mr;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author shenjie
 * @version v1.0
 * @Description
 * @Date: Create in 10:53 2018/1/17
 * @Modifide By:
 **/

//      ┏┛ ┻━━━━━┛ ┻┓
//      ┃　　　　　　 ┃
//      ┃　　　━　　　┃
//      ┃　┳┛　  ┗┳　┃
//      ┃　　　　　　 ┃
//      ┃　　　┻　　　┃
//      ┃　　　　　　 ┃
//      ┗━┓　　　┏━━━┛
//        ┃　　　┃   神兽保佑
//        ┃　　　┃   代码无BUG！
//        ┃　　　┗━━━━━━━━━┓
//        ┃　　　　　　　    ┣┓
//        ┃　　　　         ┏┛
//        ┗━┓ ┓ ┏━━━┳ ┓ ┏━┛
//          ┃ ┫ ┫   ┃ ┫ ┫
//          ┗━┻━┛   ┗━┻━┛

public class FlowBean implements Writable {
    private String phone;
    private long upFlow;
    private long dFlow;
    private long sumFlow;

    public FlowBean() {
    }

    public FlowBean(long upFlow, long dFlow) {
        this.upFlow = upFlow;
        this.dFlow = dFlow;
        this.sumFlow = upFlow + dFlow;
    }

    public FlowBean(String phone, long upFlow, long dFlow) {
        this.phone = phone;
        this.upFlow = upFlow;
        this.dFlow = dFlow;
        this.sumFlow = upFlow + dFlow;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(dFlow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        upFlow = dataInput.readLong();
        dFlow = dataInput.readLong();
    }

    @Override
    public String toString() {
        return "FlowBean{" +
                "phone='" + phone + '\'' +
                ", upFlow=" + upFlow +
                ", dFlow=" + dFlow +
                ", sumFlow=" + sumFlow +
                '}';
    }

    public String getPhone() {
        return phone;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public long getdFlow() {
        return dFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }
}
