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
public class UserBasedRecommender extends BaseRecommender {
    private final int k;
    
    public UserBasedRecommender(int k) {
        super();
        this.k = k;
    }

    @Override
    protected int innerRun() throws Exception {
        
        calculateSimilarity();
        
        calculateUUUI();
        
        recommend(DataSetConfig.getUserBasedMatrix(k));
        
//        drawMatrix();
        
        return 0;
    }
    
    private void calculateUUUI() throws Exception {
        int itemCount = itemCount();
        int userCount = userCount();
        int n1 = userCount;
        int n2 = n1;
        int n3 = itemCount;
        int type = MultiplyNearestNeighborJob.TYPE_USER_BASED;
        Path multipyerPath = DataSetConfig.getItemUserVectorPath();
        MultiplyNearestNeighborJob multiplyNearestNeighborJob = new MultiplyNearestNeighborJob(n1,
                n2, n3, multipyerPath, type, k);
        runJob(multiplyNearestNeighborJob, new Path(DataSetConfig.getUserSimilarityPath(), "rowVector"),
                DataSetConfig.getUserBasedMatrix(k), true);
    }
    
    private void calculateSimilarity() throws Exception {
        int itemCount = itemCount();
        int userCount = userCount();
        CosineSimilarityJob similarityJob = new CosineSimilarityJob(userCount,
                itemCount, userCount, DataSetConfig.getUserItemVectorPath());
        runJob(similarityJob, DataSetConfig.getUserItemVectorPath(),
                DataSetConfig.getUserSimilarityPath(), true);
    }
    
    private void drawMatrix() throws Exception {
        float precision = 0.001f;
        String imageFile = "img/others/user_based_uuui_matrix.png";
        String title = "uuui";
        String[] subTitles = new String[0];
        Path[] matrixDirs = new Path[] {
                DataSetConfig.getUserBasedMatrix(k)
        };
        String[] series = new String[] {
                "uuui"
        };
        boolean withZero = true;
        boolean diagonalOnly = false;
        DrawMatrixJob drawJob = new DrawMatrixJob(precision, imageFile, title, subTitles,
                matrixDirs, series, withZero, diagonalOnly);
        runJob(drawJob, new Path("test"), new Path("test"), false);
    }

}
