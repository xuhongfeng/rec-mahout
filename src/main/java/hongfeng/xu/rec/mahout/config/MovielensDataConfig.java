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
}
    
