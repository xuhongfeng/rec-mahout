/**
 * 2013-3-30
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.threshold.v4;

import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;
import hongfeng.xu.rec.mahout.hadoop.matrix.MatrixReducer;
import hongfeng.xu.rec.mahout.hadoop.similarity.CosineSimilarityJob;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.Vector;

/**
 * @author xuhongfeng
 *
 */
public class ThresholdCosineSimilarityJobV4 extends CosineSimilarityJob {
    private final int bottom;
    private final int top;

    public ThresholdCosineSimilarityJobV4(int n1, int n2, int n3,
            Path multiplyerPath, int bottom, int top) {
        super(n1, n2, n3, multiplyerPath);
        this.bottom = bottom;
        this.top = top;
    }
    
    @Override
    protected void initConf(Configuration conf) {
        super.initConf(conf);
        conf.setInt("bottom", bottom);
        conf.setInt("top", top);
    };
    
    @Override
    protected Class<? extends MatrixReducer> getMatrixReducer() {
        return MyReducer.class;
    }
    
    public static class MyReducer extends CosineSimilarityJob.CosineReducer {
        private int top;
        private int bottom;
        
        public MyReducer() {
            super();
        }
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            this.bottom = context.getConfiguration().getInt("bottom", -1);
            this.top = context.getConfiguration().getInt("top", -1);
            if (bottom==-1 || top==-1 || bottom>=top) {
                throw new RuntimeException();
            }
        }

        @Override
        protected double calculate(int i, int j, Vector vector1, Vector vector2) {
            if (i == j) {
                return 0.0;
            }
            int n = HadoopHelper.intersect(vector1, vector2);
            if (n < bottom || n > top) {
                return 0.0;
            }
            return super.calculate(i, j, vector1, vector2);
        }
    }
}
