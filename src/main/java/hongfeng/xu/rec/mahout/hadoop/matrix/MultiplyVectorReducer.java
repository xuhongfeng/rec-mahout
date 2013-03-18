/**
 * 2013-3-18
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.matrix;

import hongfeng.xu.rec.mahout.hadoop.misc.IntIntWritable;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author xuhongfeng
 *
 */
public class MultiplyVectorReducer extends Reducer<IntIntWritable, VectorPair,
    IntIntWritable, DoubleWritable> {
    private DoubleWritable doubleWritable = new DoubleWritable();
    
    @Override
    protected void reduce(IntIntWritable key, Iterable<VectorPair> value,
            Context context) throws IOException, InterruptedException {
        VectorPair vectorPair = value.iterator().next();
        double v = vectorPair.getFirst().get().dot(vectorPair.getSecond().get());
        if (v != 0.0) {
            doubleWritable.set(v);
            context.write(key, doubleWritable);
        }
    }
}
