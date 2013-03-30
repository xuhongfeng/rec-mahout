/**
 * 2013-3-17
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.threshold;

import hongfeng.xu.rec.mahout.hadoop.misc.IntDoubleWritable;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * @author xuhongfeng
 *
 */
public class ToVectorReducer extends Reducer<IntWritable, IntDoubleWritable, IntWritable, VectorWritable> {
    private final VectorWritable vectorWritable = new VectorWritable();

    public ToVectorReducer() {
        super();
    }
    
    @Override
    protected void reduce(IntWritable key, Iterable<IntDoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        int vectorSize = context.getConfiguration().getInt("vectorSize", 0);
        Vector vector = new RandomAccessSparseVector(vectorSize);
        for (IntDoubleWritable value:values) {
            vector.setQuick(value.getId(), value.getValue());
        }
        vectorWritable.set(vector);
        context.write(key, vectorWritable);
    }
}
