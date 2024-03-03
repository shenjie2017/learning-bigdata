package com.blue.hadoop.mr;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author shenjie
 * @version v1.0
 * @Description
 * @Date: Create in 14:18 2018/1/19
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

public class OrderBean implements Writable {
    public final static int TYPE_ORDER = 1;
    public final static int TYPE_PRODUCT = 2;

    private int type;
    private String id;
    private String date;
    private String pid;
    private int num;
    private String platform;
    private String pdName;
    private String pdColor;
    private double price;
    private String configParams;

    public OrderBean(){
        initObject();
    }

    private void initObject() {
        type = TYPE_ORDER;
        id = "";
        date = "";
        pid = "";
        num = 0;
        platform = "";
        pdName = "";
        pdColor = "";
        price = 0;
        configParams = "";
    }


    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public String getPdName() {
        return pdName;
    }

    public void setPdName(String pdName) {
        this.pdName = pdName;
    }

    public String getPdColor() {
        return pdColor;
    }

    public void setPdColor(String pdColor) {
        this.pdColor = pdColor;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public String getConfigParams() {
        return configParams;
    }

    public void setConfigParams(String configParams) {
        this.configParams = configParams;
    }

    @Override
    public String toString() {
        return "OrderBean{" +
                "id='" + id + '\'' +
                ", date='" + date + '\'' +
                ", pid='" + pid + '\'' +
                ", num=" + num +
                ", platform='" + platform + '\'' +
                ", pdName='" + pdName + '\'' +
                ", pdColor='" + pdColor + '\'' +
                ", price=" + price +
                ", configParams='" + configParams + '\'' +
                '}';
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(type);
        out.writeUTF(id);
        out.writeUTF(date);
        out.writeUTF(pid);
        out.writeInt(num);
        out.writeUTF(platform);
        out.writeUTF(pdName);
        out.writeUTF(pdColor);
        out.writeDouble(price);
        out.writeUTF(configParams);
    }

    public void readFields(DataInput in) throws IOException {
        type = in.readInt();
        id = in.readUTF();
        date = in.readUTF();
        pid = in.readUTF();
        num = in.readInt();
        platform = in.readUTF();
        pdName = in.readUTF();
        pdColor = in.readUTF();
        price = in.readDouble();
        configParams = in.readUTF();
    }
}
