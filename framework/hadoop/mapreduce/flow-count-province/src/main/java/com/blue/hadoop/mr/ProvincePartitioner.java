package com.blue.hadoop.mr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

/**
 * @author shenjie
 * @version v1.0
 * @Description
 * @Date: Create in 17:49 2018/1/17
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

public class ProvincePartitioner extends Partitioner<Text,FlowBean> {
    private static Map<String,Integer> provinceDict = new HashMap<String, Integer>();

    //初始化地域字段
    static {
        provinceDict.put("138123",0);
        provinceDict.put("138125",1);
        provinceDict.put("138134",2);
    }

    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        String prefix = text.toString().substring(0,6);
        Integer index = provinceDict.get(prefix);
        return index==null?provinceDict.size():index;
    }
}
