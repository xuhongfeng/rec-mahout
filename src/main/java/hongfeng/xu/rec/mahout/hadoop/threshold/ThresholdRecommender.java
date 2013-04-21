/**
 * 2013-3-29
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.threshold;

import hongfeng.xu.rec.mahout.config.DataSetConfig;
import hongfeng.xu.rec.mahout.hadoop.matrix.MultiplyMatrixAverageJob;
import hongfeng.xu.rec.mahout.hadoop.matrix.MultiplyNearestNeighborJob;
import hongfeng.xu.rec.mahout.hadoop.recommender.BaseRecommender;
import hongfeng.xu.rec.mahout.hadoop.similarity.ThresholdCosineSimilarityJob;

import org.apache.hadoop.fs.Path;

/**
 * @author xuhongfeng
 *
 */
public class ThresholdRecommender extends BaseRecommender {
    private final int threshold, k;
    
    public ThresholdRecommender(int threshold, int k) {
        super();
        this.threshold = threshold;
        this.k = k;
    }

    protected int innerRun() throws Exception {
        calculateThresholdSimilarity();
        
        calculateThresholdAverageSimilarity();
        
        calculateUUThreshold();
        
        calculateUIThreshold();
        
        recommend(DataSetConfig.getUUUIThresholdPath(threshold));
        
        return 0;
    }
    
    private void calculateThresholdSimilarity() throws Exception {
        Path multiplyerPath = DataSetConfig.getUserItemVectorPath();
        ThresholdCosineSimilarityJob thresholdCosineSimilarityJob =
                new ThresholdCosineSimilarityJob(userCount(), itemCount(), userCount(),
                        multiplyerPath, threshold);
        runJob(thresholdCosineSimilarityJob, DataSetConfig.getUserItemVectorPath(),
                DataSetConfig.getUserSimilarityThresholdPath(threshold), true);
    }
    
    private void calculateThresholdAverageSimilarity () throws Exception {
        Path similarityVectorPath = new Path(DataSetConfig.getUserSimilarityThresholdPath(threshold), "rowVector");
        MultiplyMatrixAverageJob matrixAverageJob = new MultiplyMatrixAverageJob(userCount(), userCount(),
                userCount(), similarityVectorPath);
        runJob(matrixAverageJob, similarityVectorPath, DataSetConfig.getUserSimilarityThresholdAveragePath(threshold)
                , true);
    }
    
    private void calculateUUThreshold() throws Exception {
        Path averageSimilarityPath = new Path(DataSetConfig.getUserSimilarityThresholdAveragePath(threshold), "rowVector");
        MultiplyThresholdMatrixJob multiplyThresholdMatrixJob =
                new MultiplyThresholdMatrixJob(userCount(), itemCount(), userCount(),
                        DataSetConfig.getUserItemVectorPath(),
                        threshold, averageSimilarityPath);
        runJob(multiplyThresholdMatrixJob, DataSetConfig.getUserItemVectorPath(),
                DataSetConfig.getUUThresholdPath(threshold), true);
    }
    
    private void calculateUIThreshold() throws Exception {
        int type = MultiplyNearestNeighborJob.TYPE_USER_BASED;
        Path multipyerPath = DataSetConfig.getItemUserVectorPath();
        Path input = new Path(DataSetConfig.getUUThresholdPath(threshold), "rowVector");
        MultiplyNearestNeighborJob multiplyNearestNeighborJob = new MultiplyNearestNeighborJob(userCount(),
                userCount(), itemCount(), multipyerPath, type, k);
        runJob(multiplyNearestNeighborJob, input,
                DataSetConfig.getUUUIThresholdPath(threshold), true);
    }
}
