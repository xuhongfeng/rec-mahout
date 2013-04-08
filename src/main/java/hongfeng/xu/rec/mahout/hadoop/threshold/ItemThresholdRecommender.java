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
    
    private int n1, n2, n3;

    public ItemThresholdRecommender(int threshold, int k) {
        super();
        this.threshold = threshold;
        this.k = k;
    }

    protected int innerRun() throws Exception {
        n1 = itemCount();
        n2 = userCount();
        n3 = n1;
        
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
                new ThresholdCosineSimilarityJob(n1, n2, n3, multiplyerPath, threshold);
        runJob(thresholdCosineSimilarityJob, DataSetConfig.getItemUserVectorPath(),
                DataSetConfig.getItemSimilarityThresholdPath(threshold), true);
    }
    
    private void calculateThresholdAverageSimilarity () throws Exception {
        Path similarityVectorPath = new Path(DataSetConfig.getItemSimilarityThresholdPath(threshold), "rowVector");
        MultiplyMatrixAverageJob matrixAverageJob = new MultiplyMatrixAverageJob(n1, n2, n3,
                similarityVectorPath);
        runJob(matrixAverageJob, similarityVectorPath, DataSetConfig.getItemSimilarityThresholdAveragePath(threshold)
                , true);
    }
    
    private void calculateIIThreshold() throws Exception {
        Path averageSimilarityPath = new Path(DataSetConfig.getItemSimilarityThresholdAveragePath(threshold), "rowVector");
        MultiplyThresholdMatrixJob multiplyThresholdMatrixJob =
                new MultiplyThresholdMatrixJob(n1, n2, n3, DataSetConfig.getItemUserVectorPath(),
                        threshold, averageSimilarityPath);
        runJob(multiplyThresholdMatrixJob, DataSetConfig.getItemUserVectorPath(),
                DataSetConfig.getIIThresholdPath(threshold), true);
    }
    
    private void calculateUIThreshold() throws Exception {
        int itemCount = itemCount();
        int userCount = userCount();
        int n1 = userCount;
        int n2 = itemCount;
        int n3 = n2;
        int type = MultiplyNearestNeighborJob.TYPE_SECOND;
        Path multipyerPath = new Path(DataSetConfig.getIIThresholdPath(threshold), "rowVector");
        Path input = DataSetConfig.getUserItemVectorPath();
        MultiplyNearestNeighborJob multiplyNearestNeighborJob = new MultiplyNearestNeighborJob(n1,
                n2, n3, multipyerPath, type, k);
        runJob(multiplyNearestNeighborJob, input,
                DataSetConfig.getUIIIThresholdPath(threshold), true);
    }
}
