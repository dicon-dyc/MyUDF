package com.dicon;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * @author dyc
 * @date 2023/05/23
 */

@Description(name = "mysum",value = "_FUNC_(x) - return the  arregation result of the column x ")
public class GenericUDAFSum extends AbstractGenericUDAFResolver {


    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {

        //判断输入参数,若输入参数不为1则抛出异常。
        if (info.length != 1){
            throw new UDFArgumentTypeException(info.length-1, "Exactly one argument is expected!");
        }

        //ObjectInspector的作用是告诉hive输入输出的数据类型，以便hive将hql转为mr程序。
        if (info[0].getCategory() != ObjectInspector.Category.PRIMITIVE){

            throw new UDFArgumentTypeException(0,"Only primitive type arguments are accepted but"
            + info[0].getTypeName()+"is passed.");

        }

        switch (((PrimitiveTypeInfo)info[0]).getPrimitiveCategory()){

            case BYTE:
            case SHORT:
            case INT:
            case LONG:
                return new GenericUDAFSumLong();
            case TIMESTAMP:
            case FLOAT:
            case STRING:
            case VARCHAR:
            case CHAR:
//                return new GenericeUDAFSumDouble();
            default:
                throw new UDFArgumentTypeException(0,"Only numeric or string type arguments are accepted but"
                + info[0].getTypeName() + " is passed.");
        }

    }


}
