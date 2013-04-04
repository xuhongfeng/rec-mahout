/**
 * 2013-3-15
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.eval;

import hongfeng.xu.rec.mahout.config.DataSetConfig;
import hongfeng.xu.rec.mahout.hadoop.MultipleSequenceOutputFormat;
import hongfeng.xu.rec.mahout.hadoop.misc.IntIntWritable;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterator;

/**
 * @author xuhongfeng
 *
 */
public class HitSet {
    private final FastIDSet set = new FastIDSet();
    
    private HitSet() {
    }
    
    public void add(int userId, int itemId) {
        set.add(hash(userId, itemId));
    }
    
    private long hash(int userId, int itemId) {
        long result = userId;
        result += Integer.MAX_VALUE*result + itemId;
        return result;
    }
    
    public boolean isHit(int userId, int itemId) {
        return set.contains(hash(userId, itemId));
    }
    
    public static HitSet create(Configuration conf) throws IOException {
        HitSet set = new HitSet();
        SequenceFileDirIterator<IntIntWritable, DoubleWritable> iterator =
                new SequenceFileDirIterator<IntIntWritable, DoubleWritable>(
                        DataSetConfig.getTestDataPath(), PathType.LIST,
                        MultipleSequenceOutputFormat.FILTER, null,
                        true, conf);
        try {
            while (iterator.hasNext()) {
                Pair<IntIntWritable, DoubleWritable> pair = iterator.next();
                IntIntWritable key = pair.getFirst();
                set.add(key.getId1(), key.getId2());
            }
        } finally {
            iterator.close();
        }
        return set;
    }
}
