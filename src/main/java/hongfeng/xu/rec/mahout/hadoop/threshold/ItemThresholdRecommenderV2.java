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
public class ItemThresholdRecommenderV2 extends BaseRecommender {
    private final int threshold, k;
    
    public ItemThresholdRecommenderV2(int threshold, int k) {
        super();
        this.threshold = threshold;
        this.k = k;
    }

    protected int innerRun() throws Exception {
        calculateThresholdSimilarity();
        
        calculateAllocateMatrix();
        
        multiplyAllocateMatrix();
        
        doAllocate();
        
        calculateIIThreshold();
        
        calculateUIThreshold();
        
        recommend(DataSetConfig.getV2UIIIThresholdPath(threshold, k));
        
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
    
    private void calculateAllocateMatrix() throws Exception {
        Path similarityVectorPath = new Path(DataSetConfig.getItemSimilarityThresholdPath(threshold),
                "rowVector");
        AllocateMatrixJob allocateMatrixJob = new AllocateMatrixJob(itemCount(),
                itemCount(), itemCount(), similarityVectorPath);
        Path output = DataSetConfig.getV2ItemAllocate(threshold);
        runJob(allocateMatrixJob, similarityVectorPath, output, true);
    }
    
    private void multiplyAllocateMatrix() throws Exception {
        Path input = new Path(DataSetConfig.getV2ItemAllocate(threshold), "rowVector");
        Path multiplyerPath = new Path(DataSetConfig.getV2ItemAllocate(threshold), "columnVector");
        MultiplyMatrixAverageJob matrixAverageJob = new MultiplyMatrixAverageJob(itemCount(), itemCount(),
                itemCount(), multiplyerPath);
        runJob(matrixAverageJob, input, DataSetConfig.getV2ItemMultiplyAllocate(threshold)
                , true);
    }
    
    private void doAllocate() throws Exception {
        Path input = new Path(DataSetConfig.getItemSimilarityThresholdPath(threshold), "rowVector");
        Path multiplyer = new Path(DataSetConfig.getV2ItemMultiplyAllocate(threshold),
                "columnVector");
        Path output = DataSetConfig.getV2ItemDoAllocate(threshold);
        DoAllocateJob doAllocateJob = new DoAllocateJob(itemCount(),
                itemCount(), itemCount(), multiplyer);
        runJob(doAllocateJob, input, output, true);
    }
    
    private void calculateIIThreshold() throws Exception {
        Path doAllocatePath = new Path(DataSetConfig.getV2ItemDoAllocate(threshold)
                , "rowVector");
        MultiplyThresholdMatrixJob multiplyThresholdMatrixJob =
                new MultiplyThresholdMatrixJob(itemCount(), userCount(), itemCount(),
                        DataSetConfig.getItemUserVectorPath(),
                        threshold, doAllocatePath);
        runJob(multiplyThresholdMatrixJob, DataSetConfig.getItemUserVectorPath(),
                DataSetConfig.getV2IIThresholdPath(threshold), true);
    }
    
    private void calculateUIThreshold() throws Exception {
        int type = MultiplyNearestNeighborJob.TYPE_ITEM_BASED;
        Path multipyerPath = new Path(DataSetConfig.getV2IIThresholdPath(threshold), "columnVector");
        Path input = DataSetConfig.getUserItemVectorPath();
        MultiplyNearestNeighborJob multiplyNearestNeighborJob = new MultiplyNearestNeighborJob(userCount(),
                itemCount(), itemCount(), multipyerPath, type, k);
        runJob(multiplyNearestNeighborJob, input,
                DataSetConfig.getV2UIIIThresholdPath(threshold, k), true);
    }
}
