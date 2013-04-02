/**
 * 2013-3-11
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.util;

import hongfeng.xu.rec.mahout.config.DataSetConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.cf.taste.hadoop.TasteHadoopUtils;
import org.apache.mahout.math.map.OpenIntLongHashMap;

/**
 * @author xuhongfeng
 *
 */
public class DataUtils {

    public static OpenIntLongHashMap readItemIDIndexMap(Configuration conf) {
        return TasteHadoopUtils.readItemIDIndexMap(DataSetConfig
                .getItemIndexPath().toString(), conf);
    }
    
}
