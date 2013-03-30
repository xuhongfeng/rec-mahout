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
public class MovielensDataConfig {
    public static final int TOP_N = 100;
    
    public static Path getAllDataPath() {
        return new Path("data/movielens-100k/u.data");
    }
    
    public static Path getTrainingDataPath() {
        return new Path("data/movielens-100k/u1.base");
    }
    
    public static Path getTestDataPath() {
        return new Path("data/movielens-100k/u1.test");
    }
    
    public static Path getRootPath() {
        return new Path("movielens-100k");
    }
    
    /*************** raw data ***************************/
    public static Path getRawDataPath() {
        return new Path(getRootPath(), "rawData");
    }
    
    public static Path getRawTrainingDataPath() {
        return new Path(getRawDataPath(), "training");
    }
    
    public static Path getRawTestDataPath() {
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
        return new Path(getUserItemMatrixPath(), "itemUserVector");
    }
    
    public static Path getUserItemVectorPath() {
        return new Path(getUserItemMatrixPath(), "userItemVector");
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
    
    public static Path getUIIU() {
        return new Path(getRootPath(), "uiiu");
    }
    
    public static Path getUUUU() {
        return new Path(getRootPath(), "uuuu");
    }
    
    public static Path getUUUUCosineAverage() {
        return new Path(getRootPath(), "uuuu-cosine-average");
    }
    
    
    /********** misc **********/
    public static Path getMiscPath() {
        return new Path(getRootPath(), "misc");
    }
    
    public static Path getCountUIIUOneZeroPath() {
        return  new Path(getMiscPath(), "countUIIUOneZero");
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
    
    public static Path getUUThresholdPath() {
        return new Path(getThresholdDir(), "uu");
    }
    
    public static Path getUUUIThresholdPath() {
        return new Path(getThresholdDir(), "uuui");
    }
    
    public static Path getSimilarityThresholdPath() {
        return new Path(getThresholdDir(), "similarity");
    }
    
    public static Path getSimilarityThresholdAveragePath() {
        return new Path(getThresholdDir(), "similarity-average");
    }
    
    public static Path getThresholdEvaluate() {
        return new Path(getThresholdDir(), "evaluate");
    }
    
    public static Path getThresholdResult() {
        return new Path(getThresholdDir(), "result");
    }
}
