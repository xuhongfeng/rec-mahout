/**
 * 2013-3-30
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.similarity;

import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;
import hongfeng.xu.rec.mahout.hadoop.matrix.MatrixReducer;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.Vector;

/**
 * @author xuhongfeng
 *
 */
public class ThresholdPearsonSimilarityJob extends CosineSimilarityJob {
    private final int threshold;

    public ThresholdPearsonSimilarityJob(int n1, int n2, int n3,
            Path multiplyerPath, int threshold) {
        super(n1, n2, n3, multiplyerPath);
        this.threshold = threshold;
    }
    
    @Override
    protected void initConf(Configuration conf) {
        super.initConf(conf);
        conf.setInt("threshold", threshold);
    };
    
    @Override
    protected Class<? extends MatrixReducer> getMatrixReducer() {
        return ThresholdPearsonReducer.class;
    }
    
    public static class ThresholdPearsonReducer extends CosineSimilarityJob.CosineReducer {
        private int threshold;
        
        public ThresholdPearsonReducer() {
            super();
        }
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            this.threshold = context.getConfiguration().getInt("threshold", 0);
            if (threshold <= 0) {
                throw new RuntimeException();
            }
        }

        @Override
        protected double calculate(int i, int j, Vector vector1, Vector vector2) {
            int n = HadoopHelper.intersect(vector1, vector2);
            if (n < threshold) {
                return 0.0;
            }
            return super.calculate(i, j, vector1, vector2);
        }
    }
}
