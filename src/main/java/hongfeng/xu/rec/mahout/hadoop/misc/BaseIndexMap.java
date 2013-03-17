/**
 * 2013-3-17
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.misc;

import hongfeng.xu.rec.mahout.config.DeliciousDataConfig;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterator;

/**
 * @author xuhongfeng
 *
 */
public abstract class BaseIndexMap {
    public static enum IndexType {
        UserIndex, ItemIndex, TagIndex
    }
    
    protected final IndexType indexType;
    
    protected BaseIndexMap(IndexType indexType) {
        this.indexType = indexType;
    }
    
    protected void init(Configuration conf) throws IOException {
        Path path = getPath(indexType);
        SequenceFileIterator<IntWritable, LongWritable> iterator =
                new SequenceFileIterator<IntWritable, LongWritable>(path, true, conf);
        try {
            while (iterator.hasNext()) {
                Pair<IntWritable, LongWritable> pair = iterator.next();
                add(pair.getFirst().get(), pair.getSecond().get());
            }
        } finally {
            iterator.close();
        }
    }
    
    protected abstract void add(int index, long id);
    
    private Path getPath(IndexType type) {
        if (type == IndexType.UserIndex) {
            return DeliciousDataConfig.getUserIndexPath();
        } else if (type == IndexType.ItemIndex) {
            return DeliciousDataConfig.getItemIndexPath();
        } else if (type == IndexType.TagIndex) {
            return DeliciousDataConfig.getTagIndexPath();
        } else {
            throw new RuntimeException();
        }
    }
}
