/**
 * 2013-4-21
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.recommender;

import hongfeng.xu.rec.mahout.config.DataSetConfig;
import hongfeng.xu.rec.mahout.hadoop.matrix.MostPredictableUserbasedJob;
import hongfeng.xu.rec.mahout.hadoop.threshold.ThresholdRecommenderV2;

import org.apache.hadoop.fs.Path;

/**
 * @author xuhongfeng
 *
 */
public class ThresholdV3 extends ThresholdRecommenderV2 {
    

    public ThresholdV3(int k) {
        super(0, k);
    }

    @Override
    protected int innerRun() throws Exception {
        calculateThresholdSimilarity();
        
        calculateAllocateMatrix();
        
        multiplyAllocateMatrix();
        
        doAllocate();
        
        calculateUUThreshold();
        
        calculateUIThreshold();
        
        recommend(DataSetConfig.getV3UUUIThresholdPath());
        
        return 0;
    }
    
    private void calculateUIThreshold() throws Exception {
        Path multipyerPath = DataSetConfig.getItemUserVectorPath();
        Path input = new Path(DataSetConfig.getV2UUThresholdPath(0), "rowVector");
        MostPredictableUserbasedJob job = new MostPredictableUserbasedJob(userCount(),
                userCount(), itemCount(), multipyerPath, k);
        runJob(job, input,
                DataSetConfig.getV3UUUIThresholdPath(), true);
    }

}
