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
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterator;

/**
 * @author xuhongfeng
 *
 */
public class UserValueMap {
    private final Map<Integer, Double> map = new HashMap<Integer, Double>();
    
    private UserValueMap() {}

    private void add(int userId, int itemId, double value) {
        if (map.containsKey(userId)) {
            map.put(userId, map.get(userId) + value);
        } else {
            map.put(userId, value);
        }
    }
    
    public double getValue(int userId) {
        if (map.containsKey(userId)) {
            return map.get(userId);
        }
        return 0.0;
    }
    
    public static UserValueMap create(Configuration conf) throws IOException {
        UserValueMap map = new UserValueMap();
        SequenceFileDirIterator<IntIntWritable, DoubleWritable> iterator =
                new SequenceFileDirIterator<IntIntWritable, DoubleWritable>(
                        DataSetConfig.getTestDataPath(), PathType.LIST,
                        MultipleSequenceOutputFormat.FILTER, null,
                        true, conf);
        try {
            while (iterator.hasNext()) {
                Pair<IntIntWritable, DoubleWritable> pair = iterator.next();
                IntIntWritable key = pair.getFirst();
                DoubleWritable value = pair.getSecond();
                map.add(key.getId1(), key.getId2(), value.get());
            }
        } finally {
            iterator.close();
        }
        return map;
    }
}
