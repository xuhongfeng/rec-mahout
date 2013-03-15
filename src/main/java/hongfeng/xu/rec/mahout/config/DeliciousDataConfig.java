/**
 * 2013-3-1
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.config;

import java.io.File;

import org.apache.hadoop.fs.Path;


/**
 * @author xuhongfeng
 *
 */
public class DeliciousDataConfig {
    public static File RAW_DATA_FILE = new File("data/hetrec2011-delicious-2k/user_taggedbookmarks-timestamps.dat");
    
    public static String HDFS_RAW_DATA_PATH = "data/delicious/user-tag-bookmark-timestamp.data";
    public static String HDFS_DELICIOUS_DIR = "delicious";
    
    public static final int TOP_N = 100;
    
    public static Path getRootPath() {
        return new Path(HDFS_DELICIOUS_DIR);
    }
    
    /*************** raw data ***************************/
    public static Path getRawDataPath() {
        return new Path(getRootPath(), "rawData");
    }
    
    public static Path getUserItemPath() {
        return new Path(getRawDataPath(), "user-item");
    }
    
    public static Path getUserTagPath() {
        return new Path(getRawDataPath(), "user-tag");
    }
    
    public static Path getTestDataPath() {
        return new Path(getRawDataPath(), "test-data");
    }
    
    public static Path getItemTagPath() {
        return new Path(getRawDataPath(), "item-tag");
    }
    
    public static Path getItemCountPath() {
        return new Path(getRawDataPath(), "item-count");
    }
    
    /*************** user item matrix *******************/
    
    public static Path getUserItemMatrixPath() {
        return new Path(getRootPath(), "userItemMatrix");
    }
    
    public static Path getItemUserVectors() {
        return new Path(getUserItemMatrixPath(), "ratingMatrix");
    }
    
    public static Path getUserItemVectors() {
        return new Path(getUserItemMatrixPath(), "userVectors");
    }
    
    public static Path getUserItemMatrixIndexPath() {
        return new Path(getUserItemMatrixPath(), "itemIDIndex");
    }
    
    /***************** random recommender ********************/
    
    public static Path getRandomRecommenderDir() {
        return new Path(getRootPath(), "randomRecommender");
    }
    
    public static Path getRandomRecommenderResultPath() {
        return new Path(getRandomRecommenderDir(), "result");
    }
    
    /***************** popular recommender *********************/
    
    public static Path getPopularItemPath() {
        return new Path(getRootPath(), "popularItem");
    }
    
    /***************** evaluate result **************************/
    public static Path getEvaluatePath() {
        return new Path(getRootPath(), "evaluate");
    }
    
    public static Path getLogPath() {
        return new Path(getRootPath(), "log");
    }
}
