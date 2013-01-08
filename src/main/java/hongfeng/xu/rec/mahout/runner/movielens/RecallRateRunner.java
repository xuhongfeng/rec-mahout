/**
 * 2013-1-9
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.runner.movielens;

import hongfeng.xu.rec.mahout.runner.AbsRecallRateRunner;

import java.io.File;
import java.io.IOException;

import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;

/**
 * @author xuhongfeng
 *
 */
public class RecallRateRunner extends AbsRecallRateRunner {

    @Override
    protected FileDataModel createModel(File file) throws IOException {
        return MovieLensRunnerDelegate.getDataModel(file);
    }

    @Override
    protected String getFilePath() {
        return MovieLensRunnerDelegate.getFilePath();
    }
    
    public static void main(String[] args) {
        new RecallRateRunner().exec();
    }
}
