/**
 * 2013-4-20
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.matrix;

import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.Vector;

/**
 * @author xuhongfeng
 *
 */
public class InsertMatrixJob extends BaseMatrixJob {

    public InsertMatrixJob(int n1, int n2, int n3, Path multiplyerPath) {
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
            double v1 = vector1.get(j);
            if (v1 != 0.0) {
                return v1;
            }
            return vector2.get(i);
        }
    }
}
