/**
 * 2013-3-18
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.matrix;

import hongfeng.xu.rec.mahout.hadoop.misc.IntDoubleWritable;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * @author xuhongfeng
 *
 */
public class CombineMatrixReducer extends Reducer<IntWritable, IntDoubleWritable,
    IntWritable, VectorWritable> {
    
    private VectorWritable vectorWritable = new VectorWritable();
    private int vectorSize;
    
    public CombineMatrixReducer() {
        super();
    }
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        vectorSize = context.getConfiguration().getInt("vectorSize", 0);
    }
    
    @Override
    protected void reduce(IntWritable key, Iterable<IntDoubleWritable> value,
            Context context) throws IOException, InterruptedException {
        Vector vector = new DenseVector(vectorSize); 
        for (IntDoubleWritable v:value) {
            if (!v.isNone()) {
                vector.setQuick(v.getId(), v.getValue());
            }
        }
        vectorWritable.set(vector);
        context.write(key, vectorWritable);
    }
}
