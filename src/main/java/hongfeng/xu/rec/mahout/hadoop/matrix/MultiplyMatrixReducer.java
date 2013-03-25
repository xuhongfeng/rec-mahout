/**
 * 2013-3-18
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.matrix;

import org.apache.mahout.math.Vector;

/**
 * @author xuhongfeng
 *
 */
public class MultiplyMatrixReducer extends MatrixReducer {

    @Override
    protected double calculate(Vector vector1, Vector vector2) {
        return vector1.dot(vector2);
    }
}
