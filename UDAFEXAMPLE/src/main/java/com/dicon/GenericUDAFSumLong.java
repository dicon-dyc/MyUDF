package com.dicon;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.LongWritable;

/**
 * @author dyc
 * @date 2023-05-24
 */
public class GenericUDAFSumLong extends GenericUDAFEvaluator {

    private PrimitiveObjectInspector inputOI;
    private LongWritable result;

    static class SumLongAgg extends AbstractAggregationBuffer{

        boolean empty;
        long sum;

        @Override
        public int estimate() {
            return JavaDataModel.PRIMITIVES1 + JavaDataModel.PRIMITIVES2;
        }
    }

    //hive会调用此方法来初始实例化一个evalutor类。
    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {

        super.init(m,parameters);
        result = new LongWritable(0);
        inputOI = (PrimitiveObjectInspector) parameters[0];
        return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    }

    //返回一个用于存储中间聚合结果的对象。
    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {

        SumLongAgg result = new SumLongAgg();
        reset(result);
        return result;
    }

    @Override
    public void reset(AggregationBuffer aggregationBuffer) throws HiveException {

        SumLongAgg sumLongAgg = (SumLongAgg) aggregationBuffer;

        sumLongAgg.empty = true;

        sumLongAgg.sum = 0;
    }

    //将一行新的数据载入到聚合buffer中。
    @Override
    public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {

        try {
            merge(aggregationBuffer,objects[0]);
        }catch (NumberFormatException e){
            throw new HiveException(e);
        }

    }

    //直接调用terminate，返回aggregationbuffer中的内容。
    @Override
    public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
        return terminate(aggregationBuffer);
    }

    //将terminatePartial返回的中间部分聚合结果合并到当前聚合种
    @Override
    public void merge(AggregationBuffer aggregationBuffer, Object o) throws HiveException {

        if (o != null){

            SumLongAgg myagg = (SumLongAgg) aggregationBuffer;

            myagg.sum += PrimitiveObjectInspectorUtils.getLong(o,inputOI);
            myagg.empty = false;
        }
    }

    //返回最终结果。
    @Override
    public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
        SumLongAgg sumLongAgg = (SumLongAgg) aggregationBuffer;

        if (sumLongAgg.empty){
            return null;
        }
        result.set(sumLongAgg.sum);

        return result;
    }
}
