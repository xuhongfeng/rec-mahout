/**
 * 2013-3-17
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.matrix;

import hongfeng.xu.rec.mahout.hadoop.misc.IntDoubleWritable;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * @author xuhongfeng
 *
 */
public class ToVectorReducer extends Reducer<KeyType, IntDoubleWritable, KeyType, VectorWritable> {
    private final VectorWritable vectorWritable = new VectorWritable();

    public ToVectorReducer() {
        super();
    }
    
    @Override
    protected void reduce(KeyType key, Iterable<IntDoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        int size = 0;
        if (key.getType() == KeyType.TYPE_ROW) {
            size = context.getConfiguration().getInt("columnSize", 0);
        } else {
            size = context.getConfiguration().getInt("rowSize", 0);
        }
        Vector vector = new RandomAccessSparseVector(size);
        for (IntDoubleWritable value:values) {
            vector.setQuick(value.getId(), value.getValue());
        }
        vectorWritable.set(vector);
        context.write(key, vectorWritable);
    }
}
