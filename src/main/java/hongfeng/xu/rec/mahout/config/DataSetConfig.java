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
    public static final int REDUCE_COUNT = 23;
    
    public static final int TOP_N = 100;
    public static final boolean ONE_ZERO = true;
    
//    public static Path inputAll = new Path("data/movielens-1m/all.dat");
//    public static Path inputTraining = new Path("data/movielens-1m/training.dat");
//    public static Path inputTest = new Path("data/movielens-1m/test.dat");
//    public static Path ROOT = new Path("movielens-1m");
    
//    public static Path inputAll = new Path("data/movielens-100k/u.data");
//    public static Path inputTraining = new Path("data/movielens-100k/u1.base");
//    public static Path inputTest = new Path("data/movielens-100k/u1.test");
//    public static Path ROOT = new Path("movielens-100k");
    
//    public static Path inputAll = new Path("data/movielens-xiefeng/all.dat");
//    public static Path inputTraining = new Path("data/movielens-xiefeng/training.dat");
//    public static Path inputTest = new Path("data/movielens-xiefeng/test.dat");
//    public static Path ROOT = new Path("movielens-xiefeng");
//    
    public static Path inputAll = new Path("data/movielens/all.dat");
    public static Path inputTraining = new Path("data/movielens/training.dat");
    public static Path inputTest = new Path("data/movielens/test.dat");
    public static Path ROOT = new Path("movielens");
    
//    public static Path inputAll = new Path("data/appchina/all.dat");
//    public static Path inputTraining = new Path("data/appchina/training.dat");
//    public static Path inputTest = new Path("data/appchina/test.dat");
//    public static Path ROOT = new Path("appchina");
    
    public static Path getRootPath() {
        return ROOT;
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
    
    public static Path getUserItemVectorPath() {
        return new Path(getUserItemMatrixPath(), "rowVector");
    }
    
    public static Path getItemUserVectorPath() {
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
    
    private static Path getPredictableRateDir() {
        return new Path(getMiscPath(), "predictableRate");
    }
    
    private static Path getPredictableRateOrigin() {
        return new Path(getPredictableRateDir(), "origin");
    }
    
    public static Path getPredictableRateOriginUser() {
        return new Path(getPredictableRateOrigin(), "userBased");
    }
    public static Path getPredictableRateOriginItem() {
        return new Path(getPredictableRateOrigin(), "itemBased");
    }
    
    public static Path getCountUIIUOneZeroPath() {
        return  new Path(getMiscPath(), "countUIIUOneZero");
    }
    
    public static Path getCountIUUIOneZeroPath() {
        return  new Path(getMiscPath(), "countIUUIOneZero");
    }
    
    private static Path getIntersectDir() {
        return  new Path(getMiscPath(), "intersect");
    }
    
    public static Path getUserIntersectPath() {
        return  new Path(getIntersectDir(), "user");
    }
    
    public static Path getItemIntersectPath() {
        return  new Path(getIntersectDir(), "item");
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
    
    public static Path getItemBasedMatrix(int k) {
        Path dir = new Path(getItemBasedDir(), "matrix");
        return new Path(dir, ""+k);
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
    
    public static Path getUserBasedMatrix(int k) {
        Path dir = new Path(getUserBasedDir(), "matrix");
        return new Path(dir, ""+k);
    }
    
    /*************** Threshold ***************************/
    public static Path getThresholdDir() {
        return new Path(getRootPath(), "threshold");
    }
    
    public static Path getItemSimilarityThresholdPath(int threshold) {
        Path dir = new Path(getThresholdDir(), "itemSimilarity");
        return new Path(dir, String.valueOf(threshold));
    }
    
    public static Path getItemSimilarityThresholdAveragePath(int threshold) {
        Path dir = new Path(getThresholdDir(), "itemSimilarityAverage");
        return new Path(dir, String.valueOf(threshold));
    }
    
    public static Path getUserSimilarityThresholdPath(int threshold) {
        Path dir = new Path(getThresholdDir(), "userSimilarity");
        return new Path(dir, String.valueOf(threshold));
    }
    
    public static Path getUserSimilarityThresholdAveragePath(int threshold) {
        Path dir = new Path(getThresholdDir(), "userSimilarityAverage");
        return new Path(dir, String.valueOf(threshold));
    }
    
    public static Path getUUThresholdPath(int threshold) {
        Path dir = new Path(getThresholdDir(), "uu");
        return new Path(dir, String.valueOf(threshold));
    }
    
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
    
    public static Path getUserThresholdEvaluate(int threshold) {
        Path dir = new Path(getThresholdDir(), "evaluateUser");
        return new Path(dir, String.valueOf(threshold));
    }
    
    public static Path getUserThresholdResult(int threshold) {
        Path dir = new Path(getThresholdDir(), "resultUser");
        return new Path(dir, String.valueOf(threshold));
    }
    
    public static Path getItemThresholdEvaluate(int threshold) {
        Path dir = new Path(getThresholdDir(), "evaluateItem");
        return new Path(dir, String.valueOf(threshold));
    }
    
    public static Path getItemThresholdResult(int threshold) {
        Path dir = new Path(getThresholdDir(), "resultItem");
        return new Path(dir, String.valueOf(threshold));
    }
    
    /*************** Threshold V2 ***************************/
    public static Path getThresholdV2Dir() {
        return new Path(getRootPath(), "threshold-v2");
    }
    public static Path getV2UserAllocate(int threshold) {
        Path dir = new Path(getThresholdV2Dir(), "user-allocate");
        return new Path(dir, String.valueOf(threshold));
    }
    public static Path getV2ItemAllocate(int threshold) {
        Path dir = new Path(getThresholdV2Dir(), "item-allocate");
        return new Path(dir, String.valueOf(threshold));
    }
    public static Path getV2UserMultiplyAllocate(int threshold) {
        Path dir = new Path(getThresholdV2Dir(), "user-multiply-allocate");
        return new Path(dir, String.valueOf(threshold));
    }
    public static Path getV2ItemMultiplyAllocate(int threshold) {
        Path dir = new Path(getThresholdV2Dir(), "item-multiply-allocate");
        return new Path(dir, String.valueOf(threshold));
    }
    public static Path getV2ItemAllocateAverage(int threshold) {
        Path dir = new Path(getThresholdV2Dir(), "item-allocate-average");
        return new Path(dir, String.valueOf(threshold));
    }
    public static Path getV2UUThresholdPath(int threshold) {
        Path dir = new Path(getThresholdV2Dir(), "uu");
        return new Path(dir, String.valueOf(threshold));
    }
    public static Path getV2ItemDoAllocate(int threshold) {
        Path dir = new Path(getThresholdV2Dir(), "do-allocate-item");
        return new Path(dir, String.valueOf(threshold));
    }
    public static Path getV2UserDoAllocate(int threshold) {
        Path dir = new Path(getThresholdV2Dir(), "do-allocate");
        return new Path(dir, String.valueOf(threshold));
    }
    public static Path getV2EveIIPath(int threshold) {
        Path dir = new Path(getThresholdV2Dir(), "eve-ii");
        return new Path(dir, String.valueOf(threshold));
    }
    public static Path getV2IIThresholdPath(int threshold) {
        Path dir = new Path(getThresholdV2Dir(), "ii");
        return new Path(dir, String.valueOf(threshold));
    }
    public static Path getV2UserThresholdResult(int threshold) {
        Path dir = new Path(getThresholdV2Dir(), "resultUser");
        return new Path(dir, String.valueOf(threshold));
    }
    public static Path getV2UserThresholdEvaluate(int threshold) {
        Path dir = new Path(getThresholdV2Dir(), "evaluateUser");
        return new Path(dir, String.valueOf(threshold));
    }
    public static Path getV2UUUIThresholdPath(int threshold) {
        Path dir = new Path(getThresholdV2Dir(), "uuui");
        return new Path(dir, String.valueOf(threshold));
    }
    public static Path getV2UIIIThresholdPath(int threshold) {
        Path dir = new Path(getThresholdV2Dir(), "uiii");
        return new Path(dir, String.valueOf(threshold));
    }
    public static Path getV2ItemThresholdResult(int threshold) {
        Path dir = new Path(getThresholdV2Dir(), "resultItem");
        return new Path(dir, String.valueOf(threshold));
    }
    public static Path getV2ItemThresholdEvaluate(int threshold) {
        Path dir = new Path(getThresholdV2Dir(), "evaluateItem");
        return new Path(dir, String.valueOf(threshold));
    }
    
    //KNN
    private static Path getKNNDir() {
        return new Path(getRootPath(), "knn");
    }
    
    public static Path getKnnItemBasedDir() {
        return new Path(getKNNDir(), "itemBased");
    }
    public static Path getKnnItemBased(int k) {
        return new Path(getKnnItemBasedDir(), ""+k);
    }
    
    public static Path getKnnUserBasedDir() {
        return new Path(getKNNDir(), "userBased");
    }
    public static Path getKnnUserBased(int k) {
        return new Path(getKnnUserBasedDir(), ""+k);
    }
}
