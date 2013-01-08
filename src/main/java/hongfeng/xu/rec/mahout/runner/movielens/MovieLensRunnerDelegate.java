/**
 * 2013-1-9
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.runner.movielens;

import hongfeng.xu.rec.mahout.model.MovielensModel;

import java.io.File;
import java.io.IOException;

/**
 * @author xuhongfeng
 *
 */
public class MovieLensRunnerDelegate {
    
    public static MovielensModel getDataModel() throws IOException {
        File dataFile = new File(getFilePath());
        return getDataModel(dataFile);
    }
    
    public static MovielensModel getDataModel(File file) throws IOException {
        return new MovielensModel(file);
    }
    
    public static String getFilePath() {
        return "data/u.data";
    }
}
