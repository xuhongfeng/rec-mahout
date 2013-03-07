/**
 * 2013-3-1
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.config;

import java.io.File;

/**
 * @author xuhongfeng
 *
 */
public class DeliciousDataConfig {
    public static File RAW_DATA_FILE = new File("data/hetrec2011-delicious-2k/user_taggedbookmarks-timestamps.dat");
    
    public static String HDFS_RAW_DATA_PATH = "data/delicious/user-tag-bookmark-timestamp.data";
    public static String HDFS_USER_ITEM_COUNT = "data/delicious/user-item-count.data";
    public static String HDFS_USER_TAG_COUNT = "data/delicious/user-tag-count.data";
    public static String HDFS_ITEM_TAG_COUNT = "data/delicious/item-tag-count.data";
}
