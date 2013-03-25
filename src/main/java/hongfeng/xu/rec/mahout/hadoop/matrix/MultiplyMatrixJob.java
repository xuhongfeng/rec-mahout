/**
 * 2013-3-18
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.matrix;

import org.apache.hadoop.fs.Path;

/**
 * @author xuhongfeng
 *
 */
public class MultiplyMatrixJob extends BaseMatrixJob {

    public MultiplyMatrixJob(int n1, int n2, int n3, Path multiplyerPath) {
        super(n1, n2, n3, multiplyerPath);
    }

    @Override
    protected Class<? extends MatrixReducer> getMatrixReducer() {
        return MultiplyMatrixReducer.class;
    }
}
