/**
 * 2013-3-15
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.eval;

import hongfeng.xu.rec.mahout.config.DeliciousDataConfig;
import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;

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
public class UserValueMap {
    private final FastByIDMap<Double> map = new FastByIDMap<Double>();
    
    private UserValueMap() {}

    private void add(long userId, long itemId, double value) {
        if (map.containsKey(userId)) {
            map.put(userId, map.get(userId) + value);
        } else {
            map.put(userId, value);
        }
    }
    
    public double getValue(long userId) {
        if (map.containsKey(userId)) {
            return map.get(userId);
        }
        return 0.0;
    }
    
    public static UserValueMap create(Configuration conf) throws IOException {
        UserValueMap map = new UserValueMap();
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
