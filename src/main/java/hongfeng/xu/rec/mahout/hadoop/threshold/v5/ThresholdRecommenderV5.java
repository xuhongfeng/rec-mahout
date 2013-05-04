/**
 * 2013-3-29
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.threshold.v5;

import hongfeng.xu.rec.mahout.config.DataSetConfig;
import hongfeng.xu.rec.mahout.hadoop.matrix.MultiplyMatrixAverageJob;
import hongfeng.xu.rec.mahout.hadoop.matrix.MultiplyNearestNeighborJob;
import hongfeng.xu.rec.mahout.hadoop.recommender.BaseRecommender;
import hongfeng.xu.rec.mahout.hadoop.similarity.ThresholdCosineSimilarityJob;
import hongfeng.xu.rec.mahout.hadoop.threshold.AllocateMatrixJob;
import hongfeng.xu.rec.mahout.hadoop.threshold.DoAllocateJob;

import org.apache.hadoop.fs.Path;

/**
 * @author xuhongfeng
 *
 */
public class ThresholdRecommenderV5 extends BaseRecommender {
    protected final int threshold;
    protected final int k;
    
    public ThresholdRecommenderV5(int threshold, int k) {
        super();
        this.threshold = threshold;
        this.k = k;
    }

    protected int innerRun() throws Exception {
        calculateThresholdSimilarity();
        
        calculateAllocateMatrix();
        
        multiplyAllocateMatrix();
        
        doAllocate();
        
        calculateUUThreshold();
        
        calculateUIThreshold();
        
        recommend(DataSetConfig.getV5UI(threshold, k));
        
        return 0;
    }
    
    protected void calculateThresholdSimilarity() throws Exception {
        Path multiplyerPath = DataSetConfig.getUserItemVectorPath();
        ThresholdCosineSimilarityJob thresholdCosineSimilarityJob =
                new ThresholdCosineSimilarityJob(userCount(), itemCount(), userCount(),
                        multiplyerPath, threshold);
        runJob(thresholdCosineSimilarityJob, DataSetConfig.getUserItemVectorPath(),
                DataSetConfig.getUserSimilarityThresholdPath(threshold), true);
    }
    
    protected void calculateAllocateMatrix() throws Exception {
        Path similarityVectorPath = new Path(DataSetConfig.getUserSimilarityThresholdPath(threshold),
                "rowVector");
        AllocateMatrixJob allocateMatrixJob = new AllocateMatrixJob(userCount(),
                userCount(), userCount(), similarityVectorPath);
        Path output = DataSetConfig.getV2UserAllocate(threshold);
        runJob(allocateMatrixJob, similarityVectorPath, output, true);
    }
    
    protected void multiplyAllocateMatrix() throws Exception {
        Path input = new Path(DataSetConfig.getV2UserAllocate(threshold), "rowVector");
        Path multiplyerPath = new Path(DataSetConfig.getV2UserAllocate(threshold), "columnVector");
        MultiplyMatrixAverageJob matrixAverageJob = new MultiplyMatrixAverageJob(userCount(), userCount(),
                userCount(), multiplyerPath);
        runJob(matrixAverageJob, input, DataSetConfig.getV2UserMultiplyAllocate(threshold)
                , true);
    }
    
    protected void doAllocate() throws Exception {
        Path input = new Path(DataSetConfig.getUserSimilarityThresholdPath(threshold), "rowVector");
        Path multiplyer = new Path(DataSetConfig.getV2UserMultiplyAllocate(threshold),
                "columnVector");
        Path output = DataSetConfig.getV2UserDoAllocate(threshold);
        DoAllocateJob doAllocateJob = new DoAllocateJob(userCount(),
                userCount(), userCount(), multiplyer);
        runJob(doAllocateJob, input, output, true);
    }
    
    protected void calculateUUThreshold() throws Exception {
        Path doAllocatePath = new Path(DataSetConfig.getV2UserDoAllocate(threshold)
                , "rowVector");
        MultiplyThresholdMatrixJobV5 multiplyThresholdMatrixJob =
                new MultiplyThresholdMatrixJobV5(userCount(), itemCount(), userCount(),
                        DataSetConfig.getUserItemVectorPath(),
                        threshold, doAllocatePath);
        runJob(multiplyThresholdMatrixJob, DataSetConfig.getUserItemVectorPath(),
                DataSetConfig.getV5UU(threshold), true);
    }
    
    private void calculateUIThreshold() throws Exception {
        int type = MultiplyNearestNeighborJob.TYPE_USER_BASED;
        Path multipyerPath = DataSetConfig.getItemUserVectorPath();
        Path input = new Path(DataSetConfig.getV5UU(threshold), "rowVector");
        MultiplyNearestNeighborJob multiplyNearestNeighborJob = new MultiplyNearestNeighborJob(userCount(),
                userCount(), itemCount(), multipyerPath, type, k);
        runJob(multiplyNearestNeighborJob, input,
                DataSetConfig.getV5UI(threshold, k), true);
    }
}
