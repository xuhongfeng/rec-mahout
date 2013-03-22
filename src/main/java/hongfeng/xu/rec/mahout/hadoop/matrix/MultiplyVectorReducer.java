/**
 * 2013-3-18
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.matrix;

import hongfeng.xu.rec.mahout.config.DeliciousDataConfig;
import hongfeng.xu.rec.mahout.hadoop.misc.IntIntWritable;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * @author xuhongfeng
 *
 */
public abstract class MultiplyVectorReducer extends Reducer<IntWritable, VectorWritable,
    IntIntWritable, DoubleWritable> {
    private VectorCache vectorCache;
    private Configuration conf;
    
    private DoubleWritable valueWritable = new DoubleWritable();
    private IntIntWritable keyWritable = new IntIntWritable();
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        conf = context.getConfiguration();
        vectorCache = VectorCache.create(getSecondColumnSize(conf), getColumnSize(conf),
                getMultiplyerPath(), context.getConfiguration());
    }
    
    private int columnSize = -1;
    private int columnSize() throws IOException {
        if (columnSize == -1) {
            columnSize = getColumnSize(conf);
        }
        return columnSize;
    }
    
    public MultiplyVectorReducer() {
        super();
    }
    
    @Override
    protected void reduce(IntWritable key, Iterable<VectorWritable> value,
            Context context) throws IOException, InterruptedException {
        keyWritable.setId1(key.get());
        Vector vector = value.iterator().next().get();
        int columnSize = columnSize();
        for (int i=0; i<columnSize; i++) {
            double v = vector.dot(vectorCache.get(i));
            if (v != 0) {
                keyWritable.setId2(i);
                valueWritable.set(v);
                context.write(keyWritable, valueWritable);
            }
        }
    }
    
    protected abstract int getColumnSize(Configuration conf) throws IOException;
    protected abstract int getSecondColumnSize(Configuration conf) throws IOException;
    protected abstract Path getMultiplyerPath();
    
    public static class UTTI extends MultiplyVectorReducer {

        @Override
        protected int getColumnSize(Configuration conf) throws IOException {
            return HadoopUtil.readInt(DeliciousDataConfig.getTagCountPath(), conf);
        }

        @Override
        protected Path getMultiplyerPath() {
            return DeliciousDataConfig.getItemTagVectorPath();
        }

        @Override
        protected int getSecondColumnSize(Configuration conf) throws IOException {
            return HadoopUtil.readInt(DeliciousDataConfig.getItemCountPath(), conf);
        }
    }
}
