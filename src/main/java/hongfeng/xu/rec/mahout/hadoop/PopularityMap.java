/**
 * 2013-3-15
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop;

import hongfeng.xu.rec.mahout.config.DeliciousDataConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;

/**
 * @author xuhongfeng
 *
 */
public class PopularityMap {
    private final FastByIDMap<Double> map = new FastByIDMap<Double>();

    private PopularityMap() {
    }
    
    private void add(long userId, long itemId, double value) {
        if (map.containsKey(itemId)) {
            map.put(itemId, map.get(itemId) + value);
        } else {
            map.put(itemId, value);
        }
    }
    
    public double getPopularity(long itemId) {
        if (map.containsKey(itemId)) {
            return map.get(itemId);
        }
        return 0.0;
    }
    
    public static PopularityMap create(Configuration conf) throws IOException {
        PopularityMap map = new PopularityMap();
        FSDataInputStream in = HadoopHelper.open(DeliciousDataConfig.getTestDataPath(), conf);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        String line = null;
        while ( (line=reader.readLine()) != null) {
            String[] ss = line.split("\t");
            long userId = Long.valueOf(ss[0]);
            long itemId = Long.valueOf(ss[0]);
            double value = Double.valueOf(ss[2]);
            map.add(userId, itemId, value);
        }
        return map;
    }
}
