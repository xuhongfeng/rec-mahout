/**
 * 2013-3-15
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.eval;

import hongfeng.xu.rec.mahout.config.DeliciousDataConfig;
import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterator;
import org.apache.mahout.math.VectorWritable;

/**
 * @author xuhongfeng
 *
 */
public class PopularityMap {
    private Map<Integer, Double> map = new HashMap<Integer, Double>();

    private PopularityMap() {
    }
    
    private void add(int itemId, double value) {
        if (map.containsKey(itemId)) {
            map.put(itemId, map.get(itemId) + value);
        } else {
            map.put(itemId, value);
        }
    }
    
    public double getPopularity(int itemId) {
        if (map.containsKey(itemId)) {
            return map.get(itemId);
        }
        return 0.0;
    }
    
    public static PopularityMap create(Configuration conf) throws IOException {
        PopularityMap map = new PopularityMap();
        SequenceFileDirIterator<IntWritable, VectorWritable> iterator =
                HadoopHelper.openVectorIterator(DeliciousDataConfig.getItemUserVectorPath(), conf);
        while (iterator.hasNext()) {
            Pair<IntWritable, VectorWritable> pair = iterator.next();
            map.add(pair.getFirst().get(), pair.getSecond().get().zSum());
        }
        iterator.close();
        return map;
    }
}
