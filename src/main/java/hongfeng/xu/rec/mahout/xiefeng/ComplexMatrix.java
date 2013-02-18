/**
 * 2013-1-15
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.xiefeng;

import org.apache.commons.math.Field;
import org.apache.commons.math.complex.Complex;
import org.apache.commons.math.linear.FieldMatrix;
import org.apache.commons.math.linear.SparseFieldMatrix;

/**
 * @author xuhongfeng
 *
 */
public class ComplexMatrix extends SparseFieldMatrix<Complex> {

    public ComplexMatrix(Field<Complex> field, int rowDimension,
            int columnDimension) throws IllegalArgumentException {
        super(field, rowDimension, columnDimension);
    }

    public ComplexMatrix(Field<Complex> field) {
        super(field);
    }

    public ComplexMatrix(FieldMatrix<Complex> other) {
        super(other);
    }

    public ComplexMatrix(SparseFieldMatrix<Complex> other) {
        super(other);
    }

}