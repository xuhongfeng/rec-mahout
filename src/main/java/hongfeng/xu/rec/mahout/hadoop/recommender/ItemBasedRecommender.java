/**
 * 2013-3-24
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.recommender;

import hongfeng.xu.rec.mahout.config.DataSetConfig;
import hongfeng.xu.rec.mahout.hadoop.matrix.DrawMatrixJob;
import hongfeng.xu.rec.mahout.hadoop.matrix.MultiplyNearestNeighborJob;
import hongfeng.xu.rec.mahout.hadoop.similarity.CosineSimilarityJob;

import org.apache.hadoop.fs.Path;

/**
 * @author xuhongfeng
 *
 */
public class ItemBasedRecommender extends BaseRecommender {
    private final int k;
    
    public ItemBasedRecommender(int k) {
        super();
        this.k = k;
    }

    @Override
    protected int innerRun() throws Exception {
        
        calculateSimilarity();
        
        calculateUIII();
        
        recommend(DataSetConfig.getItemBasedMatrix());
        
//        drawMatrix();
        
        return 0;
    }
    
    private void calculateUIII() throws Exception {
        int itemCount = itemCount();
        int userCount = userCount();
        int n1 = userCount;
        int n2 = itemCount;
        int n3 = n2;
        int type = MultiplyNearestNeighborJob.TYPE_SECOND;
        Path multipyerPath = new Path(DataSetConfig.getItemSimilarityPath(), "rowVector");
        MultiplyNearestNeighborJob multiplyNearestNeighborJob = new MultiplyNearestNeighborJob(n1,
                n2, n3, multipyerPath, type, k);
        runJob(multiplyNearestNeighborJob, DataSetConfig.getUserItemVectorPath(),
                DataSetConfig.getItemBasedMatrix(), true);
    }
    
    private void calculateSimilarity() throws Exception {
        int itemCount = itemCount();
        int userCount = userCount();
        CosineSimilarityJob similarityJob = new CosineSimilarityJob(itemCount,
                userCount, itemCount, DataSetConfig.getItemUserVectorPath());
        runJob(similarityJob, DataSetConfig.getItemUserVectorPath(),
                DataSetConfig.getItemSimilarityPath(), true);
    }
    
    private void drawMatrix() throws Exception {
        float precision = 0.001f;
        String imageFile = "img/others/item_based_uuui_matrix.png";
        String title = "uiii";
        String[] subTitles = new String[0];
        Path[] matrixDirs = new Path[] {
                DataSetConfig.getItemBasedMatrix()
        };
        String[] series = new String[] {
                "uiii"
        };
        boolean withZero = true;
        boolean diagonalOnly = false;
        DrawMatrixJob drawJob = new DrawMatrixJob(precision, imageFile, title, subTitles,
                matrixDirs, series, withZero, diagonalOnly);
        runJob(drawJob, new Path("test"), new Path("test"), false);
    }

}
