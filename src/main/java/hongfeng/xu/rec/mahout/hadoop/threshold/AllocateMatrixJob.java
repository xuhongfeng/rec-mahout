/**
 * 2013-4-11
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.threshold;

import hongfeng.xu.rec.mahout.hadoop.matrix.BaseMatrixJob;
import hongfeng.xu.rec.mahout.hadoop.matrix.MatrixReducer;

import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.Vector;

/**
 * @author xuhongfeng
 *
 */
public class AllocateMatrixJob extends BaseMatrixJob {

    public AllocateMatrixJob(int n1, int n2, int n3, Path multiplyerPath) {
        super(n1, n2, n3, multiplyerPath);
    }

    @Override
    protected Class<? extends MatrixReducer> getMatrixReducer() {
        return MyReducer.class;
    }

    public static class MyReducer extends MatrixReducer {

        @Override
        protected double calculate(int i, int j, Vector vector1, Vector vector2) {
            if (i == j) {
                return 0.0;
            }
            double v = vector1.getQuick(j);
//            HadoopHelper.log(this, "v=" + v + ", zsum=" + vector1.zSum());
            if (v == 0.0) {
                return 0.0;
            }
            
            return v/vector1.zSum();
        }
    }
}