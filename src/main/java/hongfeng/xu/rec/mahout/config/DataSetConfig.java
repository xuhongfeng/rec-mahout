/**
 * 2013-3-26
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.config;

import org.apache.hadoop.fs.Path;

/**
 * @author xuhongfeng
 *
 */
public class DataSetConfig {
    public static final int TOP_N = 100;
    
    public static Path inputAll = new Path("data/movielens-100k/u.data");
    public static Path inputTraining = new Path("data/movielens-100k/u1.base");
    public static Path inputTest = new Path("data/movielens-100k/u1.test");
    
    public static Path getRootPath() {
        return new Path("movielens-100k");
    }
    
    /*************** raw data ***************************/
    public static Path getRawDataPath() {
        return new Path(getRootPath(), "rawData");
    }
    
    public static Path getAllDataPath() {
        return new Path(getRawDataPath(), "all");
    }
    
    public static Path getTrainingDataPath() {
        return new Path(getRawDataPath(), "training");
    }
    
    public static Path getTestDataPath() {
        return new Path(getRawDataPath(), "test");
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

    /***************** id index path ****************************/
    public static Path getIdIndexPath() {
        return new Path(getRootPath(), "idIndex");
    }
    
    public static Path getUserIndexPath() {
        return new Path(getIdIndexPath(), "userIDIndex");
    }
    
    public static Path getItemIndexPath() {
        return new Path(getIdIndexPath(), "itemIDIndex");
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
        return new Path(getUserItemMatrixPath(), "rowVector");
    }
    
    public static Path getUserItemVectorPath() {
        return new Path(getUserItemMatrixPath(), "columnVector");
    }
    
    /*************** one-zero matrix *******************/
    public static Path getOneZeroMatrixPath() {
        return new Path(getMatrixPath(), "one-zero");
    }
    
    public static Path getUserItemOneZeroVectorPath() {
        return new Path(getOneZeroMatrixPath(), "userItemVector");
    }
    
    public static Path getItemUserOneZeroVectorPath() {
        return new Path(getOneZeroMatrixPath(), "itemUserVector");
    }
    
    /********** multiply matrix **********/
    public static Path getUIIUOneZero() {
        return new Path(getRootPath(), "uiiu-one-zero");
    }
    
    public static Path getIUUIOneZero() {
        return new Path(getRootPath(), "iuui-one-zero");
    }
    
    public static Path getUIIU() {
        return new Path(getRootPath(), "uiiu");
    }
    
    public static Path getUUUU() {
        return new Path(getRootPath(), "uuuu");
    }
    
    public static Path getUUUUSimilarityAverage() {
        return new Path(getRootPath(), "uuuu-similarity-average");
    }
    
    public static Path getIIIISimilarityAverage() {
        return new Path(getRootPath(), "iiii-similarity-average");
    }
    
    
    /********** misc **********/
    public static Path getMiscPath() {
        return new Path(getRootPath(), "misc");
    }
    
    public static Path getCountUIIUOneZeroPath() {
        return  new Path(getMiscPath(), "countUIIUOneZero");
    }
    
    public static Path getCountIUUIOneZeroPath() {
        return  new Path(getMiscPath(), "countIUUIOneZero");
    }
    
    /*************** similarity ***************************/
    public static Path getSimilarityPath() {
        return new Path(getRootPath(), "similarity");
    }
    
    public static Path getUserSimilarityPath() {
        return new Path(getSimilarityPath(), "user");
    }
    
    public static Path getItemSimilarityPath() {
        return new Path(getSimilarityPath(), "item");
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
    
    /*************** Threshold ***************************/
    public static Path getThresholdDir() {
        return new Path(getRootPath(), "threshold");
    }
    
    public static Path getSimilarityThresholdPath(int threshold) {
        Path dir = new Path(getThresholdDir(), "similarity");
        return new Path(dir, String.valueOf(threshold));
    }
    
    public static Path getSimilarityThresholdAveragePath(int threshold) {
        Path dir = new Path(getThresholdDir(), "similarityAverage");
        return new Path(dir, String.valueOf(threshold));
    }
    
    public static Path getUUThresholdPath(int threshold) {
        Path dir = new Path(getThresholdDir(), "uu");
        return new Path(dir, String.valueOf(threshold));
    }
    
    //TODO
    
    public static Path getIIThresholdPath(int threshold) {
        Path dir = new Path(getThresholdDir(), "ii");
        return new Path(dir, String.valueOf(threshold));
    }
    
    public static Path getUIIIThresholdPath(int threshold) {
        Path dir = new Path(getThresholdDir(), "uiii");
        return new Path(dir, String.valueOf(threshold));
    }
    
    public static Path getUUUIThresholdPath(int threshold) {
        Path dir = new Path(getThresholdDir(), "uuui");
        return new Path(dir, String.valueOf(threshold));
    }
    
    public static Path getThresholdEvaluate(int threshold) {
        Path dir = new Path(getThresholdDir(), "evaluate");
        return new Path(dir, String.valueOf(threshold));
    }
    
    public static Path getThresholdResult(int threshold) {
        Path dir = new Path(getThresholdDir(), "result");
        return new Path(dir, String.valueOf(threshold));
    }
}
