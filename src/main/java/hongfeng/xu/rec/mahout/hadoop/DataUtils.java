/**
 * 2013-3-11
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
import org.apache.hadoop.fs.Path;
import org.apache.mahout.cf.taste.hadoop.TasteHadoopUtils;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.math.map.OpenIntLongHashMap;

/**
 * @author xuhongfeng
 *
 */
public class DataUtils {

    public static FastIDSet parseItemIdSetFromHDFS (Configuration conf) throws IOException {
            FastIDSet itemIdSet = new FastIDSet();
            Path itemTagPath = DeliciousDataConfig.getItemTagPath();
            FSDataInputStream in = HadoopHelper.open(itemTagPath, conf);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line = null;
            while ( (line=reader.readLine()) != null) {
                long id = Long.valueOf(line.split("\t")[0]);
                itemIdSet.add(id);
            }
            in.close();
            return itemIdSet;
    }
    
    public static OpenIntLongHashMap readItemIDIndexMap(Configuration conf) {
        return TasteHadoopUtils.readItemIDIndexMap(DeliciousDataConfig
                .getUserItemMatrixIndexPath().toString(),
            conf);
    }
    
}
