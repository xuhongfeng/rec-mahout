/**
 * 2013-3-29
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.matrix;

import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;

import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.Vector;

/**
 * @author xuhongfeng
 *
 */
public class MultiplyMatrixAverageJob extends BaseMatrixJob {

    public MultiplyMatrixAverageJob(int n1, int n2, int n3, Path multiplyerPath) {
        super(n1, n2, n3, multiplyerPath);
    }

    @Override
    protected Class<? extends MatrixReducer> getMatrixReducer() {
        return MyReducer.class;
    }
    
    public static class MyReducer extends MatrixReducer {

        public MyReducer() {
            super();
        }
        
        @Override
        protected double calculate(int i, int j, Vector vector1, Vector vector2) {
            if (i == j) {
                return 0.0;
            }
            double dot = vector1.dot(vector2);
            if (dot == 0) {
                return 0;
            }
            double n = HadoopHelper.intersect(vector1, vector2);
            double v = dot/n;
            return v;
        }
    }
}
