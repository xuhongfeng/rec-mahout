/**
 * 2013-3-29
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.threshold;

import hongfeng.xu.rec.mahout.config.MovielensDataConfig;
import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;
import hongfeng.xu.rec.mahout.hadoop.matrix.MatrixReducer;
import hongfeng.xu.rec.mahout.hadoop.matrix.VectorCache;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.Vector;

/**
 * @author xuhongfeng
 *
 */
public class MultiplyThresholdMatrixJob extends BaseThreshldMatrixJob {
    
    public MultiplyThresholdMatrixJob(int n1, int n2, int n3,
            Path multiplyerPath, int threshold) {
        super(n1, n2, n3, multiplyerPath, threshold);
    }

    @Override
    protected Class<? extends MatrixReducer> getMatrixReducer() {
        return MyReducer.class;
    }

    public static class MyReducer extends MatrixReducer {
        private VectorCache uuuuCache;
        private int threshold;
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            int vectorCount = HadoopUtil.readInt(MovielensDataConfig.getUserCountPath(), conf);
            int vectorSize = vectorCount;
            threshold = conf.getInt("threshold", 0);
            Path uuuuAveragePath = new Path(MovielensDataConfig.getUUUUSimilarityAverage(), "rowVector");
            uuuuCache = VectorCache.create(vectorCount, vectorSize, uuuuAveragePath, conf);
        }

        public MyReducer() {
            super();
        }
        
        @Override
        protected double calculate(int i, int j, Vector vector1, Vector vector2) {
            if (threshold == 0) {
                throw new RuntimeException();
            }
            int n = HadoopHelper.intersect(vector1, vector2);
            if (n >= threshold) {
                return HadoopHelper.cosineSimilarity(vector1, vector2);
            } else {
                Vector v = uuuuCache.get(i);
                return v.getQuick(j);
            }
        }
    }
}
