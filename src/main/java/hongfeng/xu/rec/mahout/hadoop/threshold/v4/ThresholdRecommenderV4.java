/**
 * 2013-3-29
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.threshold.v4;

import hongfeng.xu.rec.mahout.config.DataSetConfig;
import hongfeng.xu.rec.mahout.hadoop.matrix.MultiplyMatrixAverageJob;
import hongfeng.xu.rec.mahout.hadoop.matrix.MultiplyNearestNeighborJob;
import hongfeng.xu.rec.mahout.hadoop.recommender.BaseRecommender;
import hongfeng.xu.rec.mahout.hadoop.threshold.AllocateMatrixJob;
import hongfeng.xu.rec.mahout.hadoop.threshold.DoAllocateJob;

import org.apache.hadoop.fs.Path;

/**
 * @author xuhongfeng
 *
 */
public class ThresholdRecommenderV4 extends BaseRecommender {
    protected final int bottom;
    protected final int top;
    protected final int k;
    
    public ThresholdRecommenderV4(int bottom, int top, int k) {
        super();
        this.k = k;
        this.bottom = bottom;
        this.top = top;
    }

    protected int innerRun() throws Exception {
        calculateThresholdSimilarity();
        
        calculateAllocateMatrix();
        
        multiplyAllocateMatrix();
        
        doAllocate();
        
        calculateUUThreshold();
        
        calculateUIThreshold();
        
        recommend(DataSetConfig.getV4UIPath(bottom, top, k));
        
        return 0;
    }
    
    protected void calculateThresholdSimilarity() throws Exception {
        Path multiplyerPath = DataSetConfig.getUserItemVectorPath();
        ThresholdCosineSimilarityJobV4 thresholdCosineSimilarityJob =
                new ThresholdCosineSimilarityJobV4(userCount(), itemCount(), userCount(),
                        multiplyerPath, bottom, top);
        runJob(thresholdCosineSimilarityJob, DataSetConfig.getUserItemVectorPath(),
                DataSetConfig.getV4ThresholdSimilarity(bottom, top), true);
    }
    
    protected void calculateAllocateMatrix() throws Exception {
        Path similarityVectorPath = new Path(DataSetConfig.getV4ThresholdSimilarity(bottom, top),
                "rowVector");
        AllocateMatrixJob allocateMatrixJob = new AllocateMatrixJob(userCount(),
                userCount(), userCount(), similarityVectorPath);
        Path output = DataSetConfig.getV4AllocatePath(bottom, top);
        runJob(allocateMatrixJob, similarityVectorPath, output, true);
    }
    
    protected void multiplyAllocateMatrix() throws Exception {
        Path input = new Path(DataSetConfig.getV4AllocatePath(bottom, top), "rowVector");
        Path multiplyerPath = new Path(DataSetConfig.getV4AllocatePath(bottom, top), "columnVector");
        MultiplyMatrixAverageJob matrixAverageJob = new MultiplyMatrixAverageJob(userCount(), userCount(),
                userCount(), multiplyerPath);
        runJob(matrixAverageJob, input, DataSetConfig.getV4AverageAllocatePath(bottom, top)
                , true);
    }
    
    protected void doAllocate() throws Exception {
        Path input = new Path(DataSetConfig.getV4ThresholdSimilarity(bottom, top), "rowVector");
        Path multiplyer = new Path(DataSetConfig.getV4AverageAllocatePath(bottom, top),
                "columnVector");
        Path output = DataSetConfig.getV4DoAllocatePath(bottom, top);
        DoAllocateJob doAllocateJob = new DoAllocateJob(userCount(),
                userCount(), userCount(), multiplyer);
        runJob(doAllocateJob, input, output, true);
    }
    
    protected void calculateUUThreshold() throws Exception {
        Path doAllocatePath = new Path(DataSetConfig.getV4DoAllocatePath(bottom, top)
                , "rowVector");
        MultiplyThresholdMatrixJobV4 multiplyThresholdMatrixJob =
                new MultiplyThresholdMatrixJobV4(userCount(), itemCount(), userCount(),
                        DataSetConfig.getUserItemVectorPath(),
                        bottom, top, doAllocatePath);
        runJob(multiplyThresholdMatrixJob, DataSetConfig.getUserItemVectorPath(),
                DataSetConfig.getV4UUPath(bottom, top), true);
    }
    
    private void calculateUIThreshold() throws Exception {
        int type = MultiplyNearestNeighborJob.TYPE_USER_BASED;
        Path multipyerPath = DataSetConfig.getItemUserVectorPath();
        Path input = new Path(DataSetConfig.getV4UUPath(bottom, top), "rowVector");
        MultiplyNearestNeighborJob multiplyNearestNeighborJob = new MultiplyNearestNeighborJob(userCount(),
                userCount(), itemCount(), multipyerPath, type, k);
        runJob(multiplyNearestNeighborJob, input,
                DataSetConfig.getV4UIPath(bottom, top, k), true);
    }
}
