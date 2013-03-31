/**
 * 2013-3-29
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.threshold;

import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;
import hongfeng.xu.rec.mahout.hadoop.matrix.MatrixReducer;
import hongfeng.xu.rec.mahout.hadoop.matrix.VectorCache;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.Vector;

/**
 * @author xuhongfeng
 *
 */
public class MultiplyThresholdMatrixJob extends BaseThreshldMatrixJob {
    private final Path averageSimilarityPath;
    private final int averageVectorSize;
    
    public MultiplyThresholdMatrixJob(int n1, int n2, int n3,
            Path multiplyerPath, int threshold, Path averageSimilarityPath
            , int averageVectorSize) {
        super(n1, n2, n3, multiplyerPath, threshold);
        this.averageSimilarityPath = averageSimilarityPath;
        this.averageVectorSize = averageVectorSize;
    }
    
    @Override
    protected void initConf(Configuration conf) {
        super.initConf(conf);
        conf.set("averageSimilarityPath", averageSimilarityPath.toString());
        conf.setInt("averageVectorSize", averageVectorSize);
    }

    @Override
    protected Class<? extends MatrixReducer> getMatrixReducer() {
        return MyReducer.class;
    }

    public static class MyReducer extends MatrixReducer {
        private VectorCache averageCache;
        private int averageVectorSize;
        private Path averageSimilarityPath;
        private int threshold;
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            averageSimilarityPath = new Path(conf.get("averageSimilarityPath"));
            int vectorSize = conf.getInt("averageVectorSize", -1);
            int vectorCount = vectorSize;
            threshold = conf.getInt("threshold", 0);
            averageCache = VectorCache.create(vectorCount, vectorSize,
                    new Path(averageSimilarityPath, "rowVector"), conf);
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
                Vector v = averageCache.get(i);
                return v.getQuick(j);
            }
        }
    }
}
