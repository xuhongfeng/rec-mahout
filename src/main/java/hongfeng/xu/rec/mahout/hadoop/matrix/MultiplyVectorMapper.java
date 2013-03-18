/**
 * 2013-3-17
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.matrix;

import hongfeng.xu.rec.mahout.config.DeliciousDataConfig;
import hongfeng.xu.rec.mahout.hadoop.misc.IntIntWritable;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.VectorWritable;

/**
 * @author xuhongfeng
 *
 */
public abstract class MultiplyVectorMapper extends Mapper<IntWritable, VectorWritable,
        IntIntWritable, VectorPair> {
    private VectorCache vectorCache;
    
    private IntIntWritable keyWritable = new IntIntWritable();
    private VectorPair valueWritable = new VectorPair();
    private VectorWritable vectorWritable = new VectorWritable();
    
    private Configuration conf;
    
    public MultiplyVectorMapper() {
        super();
    }
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        conf = context.getConfiguration();
        vectorCache = VectorCache.create(columnSize(), getMultiplyerPath(),
                context.getConfiguration());
    }
    
    protected abstract int getColumnSize(Configuration conf) throws IOException;
    protected abstract Path getMultiplyerPath();
    
    private int columnSize = -1;
    private int columnSize() throws IOException {
        if (columnSize == -1) {
            columnSize = getColumnSize(conf);
        }
        return columnSize;
    }
    
    @Override
    protected void map(IntWritable key, VectorWritable value, Context context)
            throws IOException, InterruptedException {
        for (int i=0; i<columnSize(); i++) {
            keyWritable.setId1(key.get());
            keyWritable.setId2(i);
            valueWritable.setFirst(value);
            vectorWritable.set(vectorCache.get(i));
            valueWritable.setSecond(vectorWritable);
            context.write(keyWritable, valueWritable);
        }
    }
    
    public static class UTTI extends MultiplyVectorMapper {

        @Override
        protected int getColumnSize(Configuration conf) throws IOException {
            return HadoopUtil.readInt(DeliciousDataConfig.getItemCountPath(), conf);
        }

        @Override
        protected Path getMultiplyerPath() {
            return DeliciousDataConfig.getItemTagVectorPath();
        }
    }
}
