/**
 * 2013-4-4
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.misc;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.VectorWritable;

import hongfeng.xu.rec.mahout.hadoop.BaseJob;

/**
 * @author xuhongfeng
 *
 */
public class UnpredictRateJob extends BaseJob {

    @Override
    protected int innerRun() throws Exception {
        return 0;
    }

    public static class MyMapper extends Mapper<IntWritable, VectorWritable,
        IntWritable, BooleanWritable> {

        public MyMapper() {
            super();
        }
        
    }
}
