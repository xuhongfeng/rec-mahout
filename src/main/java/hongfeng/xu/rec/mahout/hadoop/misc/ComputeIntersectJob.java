/**
 * 2013-4-7
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.misc;

import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;
import hongfeng.xu.rec.mahout.hadoop.matrix.BaseMatrixJob;
import hongfeng.xu.rec.mahout.hadoop.matrix.MatrixReducer;

import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.Vector;

/**
 * @author xuhongfeng
 *
 */
public class ComputeIntersectJob extends BaseMatrixJob {

    public ComputeIntersectJob(int n1, int n2, int n3, Path multiplyerPath) {
        super(n1, n2, n3, multiplyerPath);
    }

    @Override
    protected Class<? extends MatrixReducer> getMatrixReducer() {
        return ComputeIntersectReducer.class;
    }

    public static class ComputeIntersectReducer extends MatrixReducer {

        public ComputeIntersectReducer() {
            super();
        }

        @Override
        protected double calculate(int i, int j, Vector vector1, Vector vector2) {
            return HadoopHelper.intersect(vector1, vector2);
        }
        
    }
}