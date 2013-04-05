/**
 * 2013-4-4
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.misc;

import hongfeng.xu.rec.mahout.hadoop.BaseJob;
import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;
import hongfeng.xu.rec.mahout.hadoop.matrix.VectorCache;

import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.VectorWritable;

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
        private VectorCache itemUserVectorCache;
        private int userCount;
        private int itemCount;

        public MyMapper() {
            super();
        }
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
//            int userCount = HadoopUtil
//            itemUserVectorCache = VectorCache.create(, vectorSize, path, conf)
        }
        
    }
}
