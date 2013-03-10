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
    public static String HDFS_DELICIOUS_DIR = "delicious";
    public static String HDFS_OUTPUT_DIR_RAW_DATA_PARSER = HDFS_DELICIOUS_DIR + "/data";
}
