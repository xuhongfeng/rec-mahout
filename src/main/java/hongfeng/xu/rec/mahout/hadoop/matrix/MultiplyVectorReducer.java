/**
 * 2013-3-18
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.matrix;

import hongfeng.xu.rec.mahout.hadoop.misc.IntIntWritable;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * @author xuhongfeng
 *
 */
public class MultiplyVectorReducer extends Reducer<IntWritable, VectorWritable,
    IntIntWritable, DoubleWritable> {
    private VectorCache vectorCache;
    private Configuration conf;
    
    private DoubleWritable valueWritable = new DoubleWritable();
    private IntIntWritable keyWritable = new IntIntWritable();
    
    private int n2;
    private int n3;
    private Path multiplyerPath;
    
    
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        conf = context.getConfiguration();
        n2 = conf.getInt("n2", 0);
        n3 = conf.getInt("n3", 0);
        multiplyerPath = new Path(conf.get("multiplyerPath"));
        vectorCache = VectorCache.create(n3, n2, multiplyerPath, context.getConfiguration());
    }
    
    public MultiplyVectorReducer() {
        super();
    }
    
    @Override
    protected void reduce(IntWritable key, Iterable<VectorWritable> value,
            Context context) throws IOException, InterruptedException {
        keyWritable.setId1(key.get());
        Vector vector = value.iterator().next().get();
        for (int i=0; i<vectorCache.size(); i++) {
            double v = vector.dot(vectorCache.get(i));
            if (v != 0) {
                keyWritable.setId2(i);
                valueWritable.set(v);
                context.write(keyWritable, valueWritable);
            }
        }
    }
    
}
