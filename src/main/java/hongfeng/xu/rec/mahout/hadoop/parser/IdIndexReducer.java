/**
 * 2013-3-17
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.parser;

import hongfeng.xu.rec.mahout.config.DeliciousDataConfig;

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
    }
    
    private SequenceFile.Writer createWriter(int type, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path path = null;
        if (type == IdIndexMapper.TYPE_USER_ID) {
            path = DeliciousDataConfig.getUserIndexPath();
        } else if (type == IdIndexMapper.TYPE_ITEM_ID) {
            path = DeliciousDataConfig.getItemIndexPath();
        } else if (type == IdIndexMapper.TYPE_TAG_ID) {
            path = DeliciousDataConfig.getTagIndexPath();
        }
        return SequenceFile.createWriter(fs, conf, path, IntWritable.class, LongWritable.class);
    }
}
