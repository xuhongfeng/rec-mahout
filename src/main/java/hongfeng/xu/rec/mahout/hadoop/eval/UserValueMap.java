/**
 * 2013-3-15
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.eval;

import hongfeng.xu.rec.mahout.config.DataSetConfig;
import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;

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
        FSDataInputStream in = HadoopHelper.open(DataSetConfig.getTestDataPath(), conf);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        String line = null;
        while ( (line=reader.readLine()) != null) {
            String[] ss = line.split("\t");
            int userId = Integer.valueOf(ss[0]);
            int itemId = Integer.valueOf(ss[1]);
            double value = Double.valueOf(ss[2]);
            map.add(userId, itemId, value);
        }
        return map;
    }
}
