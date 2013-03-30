/**
 * 2013-3-24
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.similarity;

import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;
import hongfeng.xu.rec.mahout.hadoop.matrix.BaseMatrixJob;
import hongfeng.xu.rec.mahout.hadoop.matrix.MatrixReducer;

import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.Vector;

/**
 * @author xuhongfeng
 *
 */
public class CosineSimilarityJob extends BaseMatrixJob {
    
    public CosineSimilarityJob(int n1, int n2, int n3, Path multiplyerPath) {
        super(n1, n2, n3, multiplyerPath);
    }

    @Override
    protected Class<? extends MatrixReducer> getMatrixReducer() {
        return CosineReducer.class;
    }
    
    public static class CosineReducer extends MatrixReducer {
        
        public CosineReducer() {
            super();
        }

        @Override
        protected double calculate(int i, int j, Vector vector1, Vector vector2) {
            return HadoopHelper.cosinSimilarity(vector1, vector2);
        }
    }
}
