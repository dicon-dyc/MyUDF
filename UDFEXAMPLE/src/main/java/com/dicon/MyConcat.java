package com.dicon;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 *
 * MyConcat
 *
 * @author dyc
 * @date 2023-05-23
 */


@Description(name = "myconcat",value = "_func_(value1,value2)",extended ="EXAMPLE:\n" + "> SELECT _FUNC_(STRING) FROM TABLES;\n")
public class MyConcat extends UDF {

    /**
     * input：MyConcat(data)
     * output：MyConcat:data
     * @param data String类型的参数
     * @return 结果
     */
    public String evaluate(String data){

        return "MyConcat:"+data;

    }

    /**
     * input: MyConcat(data,param)
     * output:MyConcat:data:param
     * @param data int类型的参数
     * @param param 需要连接的参数
     * @return 结果
     */
    public String evaluate(int data,String param){

        return "MyConcat:"+data+param;
    }
}
