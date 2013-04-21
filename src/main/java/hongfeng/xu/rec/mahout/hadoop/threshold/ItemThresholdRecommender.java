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
public class ItemThresholdRecommender extends BaseRecommender {
    private final int threshold, k;
    
    public ItemThresholdRecommender(int threshold, int k) {
        super();
        this.threshold = threshold;
        this.k = k;
    }

    protected int innerRun() throws Exception {
        calculateThresholdSimilarity();
        
        calculateThresholdAverageSimilarity();
        
        calculateIIThreshold();
        
        calculateUIThreshold();
        
        recommend(DataSetConfig.getUIIIThresholdPath(threshold));
        
        return 0;
    }
    
    private void calculateThresholdSimilarity() throws Exception {
        Path multiplyerPath = DataSetConfig.getItemUserVectorPath();
        ThresholdCosineSimilarityJob thresholdCosineSimilarityJob =
                new ThresholdCosineSimilarityJob(itemCount(), userCount(), itemCount(),
                        multiplyerPath, threshold);
        runJob(thresholdCosineSimilarityJob, DataSetConfig.getItemUserVectorPath(),
                DataSetConfig.getItemSimilarityThresholdPath(threshold), true);
    }
    
    private void calculateThresholdAverageSimilarity () throws Exception {
        Path similarityVectorPath = new Path(DataSetConfig.getItemSimilarityThresholdPath(threshold), "rowVector");
        MultiplyMatrixAverageJob matrixAverageJob = new MultiplyMatrixAverageJob(itemCount(),
                itemCount(), itemCount(), similarityVectorPath);
        runJob(matrixAverageJob, similarityVectorPath, DataSetConfig.getItemSimilarityThresholdAveragePath(threshold)
                , true);
    }
    
    private void calculateIIThreshold() throws Exception {
        Path averageSimilarityPath = new Path(DataSetConfig.getItemSimilarityThresholdAveragePath(threshold), "rowVector");
        MultiplyThresholdMatrixJob multiplyThresholdMatrixJob =
                new MultiplyThresholdMatrixJob(itemCount(), userCount(), itemCount(),
                        DataSetConfig.getItemUserVectorPath(), threshold, averageSimilarityPath);
        runJob(multiplyThresholdMatrixJob, DataSetConfig.getItemUserVectorPath(),
                DataSetConfig.getIIThresholdPath(threshold), true);
    }
    
    private void calculateUIThreshold() throws Exception {
        int type = MultiplyNearestNeighborJob.TYPE_ITEM_BASED;
        Path multipyerPath = new Path(DataSetConfig.getIIThresholdPath(threshold), "columnVector");
        Path input = DataSetConfig.getUserItemVectorPath();
        MultiplyNearestNeighborJob multiplyNearestNeighborJob = new MultiplyNearestNeighborJob(
                userCount(),itemCount(), itemCount(), multipyerPath, type, k);
        runJob(multiplyNearestNeighborJob, input,
                DataSetConfig.getUIIIThresholdPath(threshold), true);
    }
}
