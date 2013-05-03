/**
 * 2013-3-29
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.threshold.v4;

import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;
import hongfeng.xu.rec.mahout.hadoop.matrix.BaseMatrixJob;
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
public class MultiplyThresholdMatrixJobV4 extends BaseMatrixJob {
    private final Path averageSimilarityPath;
    private final int bottom;
    private final int top;
    
    public MultiplyThresholdMatrixJobV4(int n1, int n2, int n3,
            Path multiplyerPath, int bottom, int top, Path averageSimilarityPath) {
        super(n1, n2, n3, multiplyerPath);
        this.averageSimilarityPath = averageSimilarityPath;
        this.bottom = bottom;
        this.top = top;
    }
    
    @Override
    protected void initConf(Configuration conf) {
        super.initConf(conf);
        conf.set("averageSimilarityPath", averageSimilarityPath.toString());
        conf.setInt("bottom", bottom);
        conf.setInt("top", top);
    }

    @Override
    protected Class<? extends MatrixReducer> getMatrixReducer() {
        return MyReducer.class;
    }

    public static class MyReducer extends MatrixReducer {
        private VectorCache averageCache;
        private Path averageSimilarityPath;
        private int top;
        private int bottom;
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            averageSimilarityPath = new Path(conf.get("averageSimilarityPath"));
            int vectorCount = conf.getInt("n1", 0);
            int vectorSize = vectorCount;
            averageCache = VectorCache.create(vectorCount, vectorSize,
                    averageSimilarityPath, conf);
            this.bottom = context.getConfiguration().getInt("bottom", -1);
            this.top = context.getConfiguration().getInt("top", -1);
            if (bottom==-1 || top==-1 || bottom>=top) {
                throw new RuntimeException();
            }
        }

        public MyReducer() {
            super();
        }
        
        @Override
        protected double calculate(int i, int j, Vector vector1, Vector vector2) {
            if (i == j) {
                return 0.0;
            }
            int n = HadoopHelper.intersect(vector1, vector2);
            if (n >= bottom && n<=top) {
                double sim = HadoopHelper.cosineSimilarity(vector1, vector2);
                if (sim != 0.0) {
                    return sim;
                }
            }
            Vector v = averageCache.get(i);
            return v.getQuick(j);
        }
    }
}
