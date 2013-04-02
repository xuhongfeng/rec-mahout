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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;

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
        FSDataInputStream in = HadoopHelper.open(DataSetConfig.getTestDataPath(), conf);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        String line = null;
        while ( (line=reader.readLine()) != null) {
            String[] ss = line.split("\t");
            int userId = Integer.valueOf(ss[0]);
            int itemId = Integer.valueOf(ss[1]);
            set.add(userId, itemId);
        }
        return set;
    }
}
