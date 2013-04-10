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
    
    private int n1, n2, n3;

    public ThresholdRecommender(int threshold, int k) {
        super();
        this.threshold = threshold;
        this.k = k;
    }

    protected int innerRun() throws Exception {
        n1 = userCount();
        n2 = itemCount();
        n3 = n1;
        
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
                new ThresholdCosineSimilarityJob(n1, n2, n3, multiplyerPath, threshold);
        runJob(thresholdCosineSimilarityJob, DataSetConfig.getUserItemVectorPath(),
                DataSetConfig.getUserSimilarityThresholdPath(threshold), true);
    }
    
    private void calculateThresholdAverageSimilarity () throws Exception {
        Path similarityVectorPath = new Path(DataSetConfig.getUserSimilarityThresholdPath(threshold), "rowVector");
        MultiplyMatrixAverageJob matrixAverageJob = new MultiplyMatrixAverageJob(n1, n1, n1,
                similarityVectorPath);
        runJob(matrixAverageJob, similarityVectorPath, DataSetConfig.getUserSimilarityThresholdAveragePath(threshold)
                , true);
    }
    
    private void calculateUUThreshold() throws Exception {
        Path averageSimilarityPath = new Path(DataSetConfig.getUserSimilarityThresholdAveragePath(threshold), "rowVector");
        MultiplyThresholdMatrixJob multiplyThresholdMatrixJob =
                new MultiplyThresholdMatrixJob(n1, n2, n3, DataSetConfig.getUserItemVectorPath(),
                        threshold, averageSimilarityPath);
        runJob(multiplyThresholdMatrixJob, DataSetConfig.getUserItemVectorPath(),
                DataSetConfig.getUUThresholdPath(threshold), true);
    }
    
    private void calculateUIThreshold() throws Exception {
        int itemCount = itemCount();
        int userCount = userCount();
        int n1 = userCount;
        int n2 = n1;
        int n3 = itemCount;
        int type = MultiplyNearestNeighborJob.TYPE_FIRST;
        Path multipyerPath = DataSetConfig.getItemUserVectorPath();
        Path input = new Path(DataSetConfig.getUUThresholdPath(threshold), "rowVector");
        MultiplyNearestNeighborJob multiplyNearestNeighborJob = new MultiplyNearestNeighborJob(n1,
                n2, n3, multipyerPath, type, k);
        runJob(multiplyNearestNeighborJob, input,
                DataSetConfig.getUUUIThresholdPath(threshold), true);
    }
}
