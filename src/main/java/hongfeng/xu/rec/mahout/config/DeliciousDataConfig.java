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
    
//    public static String HDFS_RAW_DATA_PATH = "data/delicious/user-tag-bookmark-timestamp.data";
//    public static String HDFS_DELICIOUS_DIR = "delicious";
    public static String HDFS_RAW_DATA_PATH = "data/movielens-2k/user_taggedmovies-timestamps.dat";
    public static String HDFS_DELICIOUS_DIR = "movielens-2k";
    
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
    
    /***************** itemUser-userItem matrix path ****************************/
    public static Path getIUUIPath() {
        return new Path(getRootPath(), "iuui");
    }
    
    public static Path getIUUIRawMatrixPath() {
        return new Path(getIUUIPath(), "rawMatrix");
    }
    
    public static Path getIUUIRowVectorPath() {
        return new Path(getIUUIPath(), "rowVector");
    }
    
    public static Path getIUUIColumnVectorPath() {
        return new Path(getIUUIPath(), "columnVector");
    }
    
    /***************** userItem-itemUser matrix path ****************************/
    public static Path getUIIUPath() {
        return new Path(getRootPath(), "uiiu");
    }
    
    public static Path getUIIURawMatrixPath() {
        return new Path(getUIIUPath(), "rawMatrix");
    }
    
    public static Path getUIIURowVectorPath() {
        return new Path(getUIIUPath(), "rowVector");
    }
    
    public static Path getUIIUColumnVectorPath() {
        return new Path(getUIIUPath(), "columnVector");
    }
    
    
    /***************** userTag-tagItem matrix path ****************************/
    public static Path getUTTIPath() {
        return new Path(getRootPath(), "utti");
    }
    
    public static Path getUTTIRawMatrixPath() {
        return new Path(getUTTIPath(), "rawMatrix");
    }
    
    public static Path getUTTIRowVectorPath() {
        return new Path(getUTTIPath(), "rowVector");
    }
    
    public static Path getUTTIColumnVectorPath() {
        return new Path(getUTTIPath(), "columnVector");
    }
    
    /*************** simple tag based ***************************/
    public static Path getSimpleTagBasedDir() {
        return new Path(getRootPath(), "simpleTagBased");
    }
    
    public static Path getSimpleTagBasedResult() {
        return new Path(getSimpleTagBasedDir(), "result");
    }
    
    public static Path getSimpleTagBasedEvaluate() {
        return new Path(getSimpleTagBasedDir(), "evaluate");
    }
    
    /*************** simple tag based ***************************/
    public static Path getXiefengDir() {
        return new Path(getRootPath(), "xiefeng");
    }
    
    public static Path getXiefengResult() {
        return new Path(getXiefengDir(), "result");
    }
    
    public static Path getXiefengEvaluate() {
        return new Path(getXiefengDir(), "evaluate");
    }
    
    /*************** similarity ***************************/
    public static Path getSimilarityDir() {
        return new Path(getRootPath(), "similarity");
    }
    
    public static Path getCosineSimilarityPath() {
        return new Path(getSimilarityDir(), "cosine");
    }
    
    public static Path getUserCosineSimilarityPath() {
        return new Path(getCosineSimilarityPath(), "user");
    }
    
    public static Path getItemCosineSimilarityPath() {
        return new Path(getCosineSimilarityPath(), "item");
    }
    
    /*************** UserBased ***************************/
    public static Path getUserBasedDir() {
        return new Path(getRootPath(), "userBased");
    }
    
    public static Path getUserBasedResult() {
        return new Path(getUserBasedDir(), "result");
    }
    
    public static Path getUserBasedEvaluate() {
        return new Path(getUserBasedDir(), "evaluate");
    }
    
    public static Path getUserBasedMatrix() {
        return new Path(getUserBasedDir(), "matrix");
    }
    
    /*************** ItemBased ***************************/
    public static Path getItemBasedDir() {
        return new Path(getRootPath(), "itemBased");
    }
    
    public static Path getItemBasedResult() {
        return new Path(getItemBasedDir(), "result");
    }
    
    public static Path getItemBasedEvaluate() {
        return new Path(getItemBasedDir(), "evaluate");
    }
    
    public static Path getItemBasedMatrix() {
        return new Path(getItemBasedDir(), "matrix");
    }
}
