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
    
    /*************** matrix *******************/
    public static Path getMatrixPath() {
        return new Path(getRootPath(), "matrix");
    }
    
    /*************** user item matrix *******************/
    
    public static Path getUserItemMatrixPath() {
        return new Path(getMatrixPath(), "userItemMatrix");
    }
    
    public static Path getItemUserVectorPath() {
        return new Path(getUserItemMatrixPath(), "itemUserVector");
    }
    
    public static Path getUserItemVectorPath() {
        return new Path(getUserItemMatrixPath(), "userItemVector");
    }
    
    /*************** user tag matrix *******************/
    
    public static Path getUserTagMatrixPath() {
        return new Path(getMatrixPath(), "userTagMatrix");
    }
    
    public static Path getUserTagVectorPath() {
        return new Path(getUserTagMatrixPath(), "userTagVector");
    }
    
    public static Path getTagUserVectorPath() {
        return new Path(getUserTagMatrixPath(), "tagUserVector");
    }
    
    /*************** item tag matrix *******************/
    
    public static Path getItemTagMatrixPath() {
        return new Path(getMatrixPath(), "itemTagMatrix");
    }
    
    public static Path getItemTagVectorPath() {
        return new Path(getItemTagMatrixPath(), "itemTagVector");
    }
    
    public static Path getTagItemVectorPath() {
        return new Path(getItemTagMatrixPath(), "tagItemVector");
    }
    
    /***************** random recommender ********************/
    
    public static Path getRandomRecommenderDir() {
        return new Path(getRootPath(), "randomRecommender");
    }
    
    public static Path getRandomRecommenderResultPath() {
        return new Path(getRandomRecommenderDir(), "result");
    }
    
    public static Path getRandomRecommenderEvaluate() {
        return new Path(getRandomRecommenderDir(), "evaluate");
    }
    
    /***************** popular recommender *********************/
    
    public static Path getPopularRecommenderDir() {
        return new Path(getRootPath(), "popularRecommender");
    }
    
    public static Path getPopularItemSortPath() {
        return new Path(getPopularRecommenderDir(), "popularItem");
    }
    
    public static Path getPopularRecommenderResultPath() {
        return new Path(getPopularRecommenderDir(), "result");
    }
    
    public static Path getPopularRecommederEvaluate() {
        return new Path(getPopularRecommenderDir(), "evaluate");
    }
    
    /***************** id index path ****************************/
    public static Path getIdIndexPath() {
        return new Path(getRootPath(), "idIndex");
    }
    
    public static Path getUserIndexPath() {
        return new Path(getIdIndexPath(), "userIDIndex");
    }
    
    public static Path getTagIndexPath() {
        return new Path(getIdIndexPath(), "tagIDIndex");
    }
    public static Path getItemIndexPath() {
        return new Path(getIdIndexPath(), "itemIDIndex");
    }
    
    /***************** value path ****************************/
    
    public static Path getValueDirPath() {
        return new Path(getRootPath(), "value");
    }
    
    public static Path getUserCountPath() {
        return new Path(getValueDirPath(), "user-count");
    }
    
    public static Path getItemCountPath() {
        return new Path(getValueDirPath(), "item-count");
    }
    
    public static Path getTagCountPath() {
        return new Path(getValueDirPath(), "tag-count");
    }
    
    /***************** userTag-tagUser matrix path ****************************/
    public static Path getUTTIPath() {
        return new Path(getRootPath(), "utti");
    }
    
    public static Path getUTTIRawMatrixPath() {
        return new Path(getUTTIPath(), "rawMatrix");
    }
    
    public static Path getUTTIRowVectorPath() {
        return new Path(getUTTIPath(), "rowVector");
    }
}
