/**
 * 2013-3-11
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.recommender;

import hongfeng.xu.rec.mahout.hadoop.BaseJob;

import org.apache.hadoop.fs.Path;

/**
 * @author xuhongfeng
 *
 */
public abstract class BaseRecommender extends BaseJob {
    public static final int TOP_N = 100;
    
    protected void recommend(Path matrixPath) throws Exception {
        RecommendJob recommendJob = new RecommendJob();
        runJob(recommendJob, new Path(matrixPath, "rowVector"),
                getOutputPath(), true);
    }
}
