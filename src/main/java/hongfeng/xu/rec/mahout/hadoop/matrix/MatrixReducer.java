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
public abstract class MatrixReducer extends Reducer<IntWritable, VectorWritable,
    IntIntWritable, DoubleWritable> {
    private VectorCache vectorCache;
    protected Configuration conf;
    
    private DoubleWritable valueWritable = new DoubleWritable();
    private IntIntWritable keyWritable = new IntIntWritable();
    
    private int n1;
    private int n2;
    private int n3;
    private Path multiplyerPath;
    
    
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        conf = context.getConfiguration();
        n1 = conf.getInt("n1", 0);
        n2 = conf.getInt("n2", 0);
        n3 = conf.getInt("n3", 0);
        multiplyerPath = new Path(conf.get("multiplyerPath"));
        vectorCache = VectorCache.create(n3, n2, multiplyerPath, context.getConfiguration());
    }
    
    public MatrixReducer() {
        super();
    }
    
    private boolean checked = false;
    @Override
    protected void reduce(IntWritable key, Iterable<VectorWritable> value,
            Context context) throws IOException, InterruptedException {
        int i = key.get();
        keyWritable.setId1(i);
        Vector vector1 = value.iterator().next().get();
        for (int j=0; j<vectorCache.size(); j++) {
            Vector vector2 = vectorCache.get(j);
            if (!checked) {
                if (vectorCache.size() != n3) {
                    throw new RuntimeException("n3="+n3 + ", vectorCache.size=" + vectorCache.size());
                }
                if (n2 != vector1.size()) {
                    throw new RuntimeException("n2="+n2 + ", vector1.size=" + vector1.size());
                }
                if (n2 != vector2.size()) {
                    throw new RuntimeException("n2="+n2 + ", vector2.size=" + vector2.size());
                }
                checked = false;
            }
            double v = calculate(i, j, vector1, vector2);
            if (v != 0) {
                keyWritable.setId2(j);
                valueWritable.set(v);
                context.write(keyWritable, valueWritable);
            }
        }
    }
    
    protected abstract double calculate(int i, int j, Vector vector1, Vector vector2);
}
