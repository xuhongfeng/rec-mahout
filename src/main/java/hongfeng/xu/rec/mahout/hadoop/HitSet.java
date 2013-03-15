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
import org.apache.mahout.cf.taste.impl.common.FastIDSet;

/**
 * @author xuhongfeng
 *
 */
public class HitSet {
    private final FastIDSet set = new FastIDSet();
    private int itemCount = 0;
    
    private HitSet() {
    }
    
    public void add(long userId, long itemId) {
        set.add(hash(userId, itemId));
        itemCount ++;
    }
    
    private long hash(long userId, long itemId) {
        long result = 1;
        result += result*31 + userId;
        result += result*31 + itemId;
        return result;
    }
    
    public int itemCount() {
        return itemCount;
    }
    
    public boolean isHit(long userId, long itemId) {
        return set.contains(hash(userId, itemId));
    }
    
    public static HitSet create(Configuration conf) throws IOException {
        HitSet set = new HitSet();
        FSDataInputStream in = HadoopHelper.open(DeliciousDataConfig.getTestDataPath(), conf);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        String line = null;
        while ( (line=reader.readLine()) != null) {
            String[] ss = line.split("\t");
            long userId = Long.valueOf(ss[0]);
            long itemId = Long.valueOf(ss[0]);
            set.add(userId, itemId);
        }
        return set;
    }
}
