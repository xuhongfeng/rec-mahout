/**
 * 2013-3-26
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.threshold;

import hongfeng.xu.rec.mahout.config.MovielensDataConfig;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.common.HadoopUtil;

/**
 * @author xuhongfeng
 *
 */
public class IdIndexReducer extends Reducer<IntWritable, LongWritable, NullWritable, NullWritable> {
    private final IntWritable intWritable = new IntWritable();
    private final LongWritable longWritable = new LongWritable();
    

    public IdIndexReducer() {
        super();
    }
    
    @Override
    protected void reduce(IntWritable key, Iterable<LongWritable> value,
            Context context) throws IOException, InterruptedException {
        int incrId = 0;
        Set<Long> idSet = new HashSet<Long>();
        SequenceFile.Writer writer = createWriter(key.get(), context.getConfiguration());
        try {
            for (LongWritable v:value) {
                long id = v.get();
                if (!idSet.contains(id)) {
                    idSet.add(id);
                    intWritable.set(incrId++);
                    longWritable.set(id);
                    writer.append(intWritable, longWritable);
                }
            }
        } finally {
            writer.close();
        }
        if (key.get() == IdIndexMapper.TYPE_USER_ID) {
            HadoopUtil.writeInt(incrId, MovielensDataConfig.getUserCountPath(), context.getConfiguration());
        } else if (key.get() == IdIndexMapper.TYPE_ITEM_ID) {
            HadoopUtil.writeInt(incrId, MovielensDataConfig.getItemCountPath(), context.getConfiguration());
        } else {
            throw new RuntimeException();
        }
    }
    
    private SequenceFile.Writer createWriter(int type, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path path = null;
        if (type == IdIndexMapper.TYPE_USER_ID) {
            path = MovielensDataConfig.getUserIndexPath();
        } else if (type == IdIndexMapper.TYPE_ITEM_ID) {
            path = MovielensDataConfig.getItemIndexPath();
        } else {
            throw new RuntimeException();
        }
        return SequenceFile.createWriter(fs, conf, path, IntWritable.class, LongWritable.class);
    }
}