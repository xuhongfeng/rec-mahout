/**
 * 2013-4-21
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout;

import hongfeng.xu.rec.mahout.chart.XYChartDrawer;
import hongfeng.xu.rec.mahout.config.DataSetConfig;
import hongfeng.xu.rec.mahout.hadoop.BaseJob;
import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;
import hongfeng.xu.rec.mahout.hadoop.eval.EvaluateRecommenderJob;
import hongfeng.xu.rec.mahout.hadoop.recommender.ItemBasedRecommender;
import hongfeng.xu.rec.mahout.hadoop.recommender.UserBasedRecommender;
import hongfeng.xu.rec.mahout.structure.TypeAndNWritable;
import hongfeng.xu.rec.mahout.util.L;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterator;

/**
 * @author xuhongfeng
 *
 */
public class KNN extends BaseJob {
    private final int N = 20;
    private static final int WIDTH = 600;
    private static final int HEIGHT = 300;

    @Override
    protected int innerRun() throws Exception {
        
        evaluateUserBased();
        evaluateItemBased();
        
        return 0;
    }
    
    private void evaluateItemBased() throws Exception {
        int[] KList = new int[] {50, 80, 100,
                150, 200, 250, 300, 400, 500, 600, 700, 800, 900, 1000, 1300, 1500};
        for (int k:KList) {
            Path knnDir = DataSetConfig.getKnnItemBased(k);
            Path resultPath = getResultPath(knnDir);
            Path evaluatePath = getEvaluatePath(knnDir);
            ItemBasedRecommender recommender = new ItemBasedRecommender(k);
            EvaluateRecommenderJob<ItemBasedRecommender> job =
                    new EvaluateRecommenderJob<ItemBasedRecommender> (recommender,
                            resultPath);
            runJob(job, DataSetConfig.getUserItemVectorPath(), evaluatePath, true);
        }
        
        draw(DataSetConfig.getKnnItemBasedDir(), "ItemBased", KList);
    }
    
    private void evaluateUserBased() throws Exception {
        int[] KList = new int[] {10, 20, 30, 40, 50, 60, 70, 80, 90, 100,
                150, 200, 250, 300, 400, 500, 600, 700, 800, 900};
        for (int k:KList) {
            Path knnDir = DataSetConfig.getKnnUserBased(k);
            Path resultPath = getResultPath(knnDir);
            Path evaluatePath = getEvaluatePath(knnDir);
            UserBasedRecommender recommender = new UserBasedRecommender(k);
            EvaluateRecommenderJob<UserBasedRecommender> job =
                    new EvaluateRecommenderJob<UserBasedRecommender> (recommender,
                            resultPath);
            runJob(job, DataSetConfig.getUserItemVectorPath(), evaluatePath, true);
        }
        
        draw(DataSetConfig.getKnnUserBasedDir(), "UserBased", KList);
    }
    
    private void draw(Path knnDir, String titlePrefix, int[] KList) throws IOException {
        double[][] precisionValues = new double[2][KList.length];
        double[][] recallValues = new double[2][KList.length];
        double[][] coverageValues = new double[2][KList.length];
        double[][] popularityValues = new double[2][KList.length];
        
        for (int i=0; i<KList.length; i++) {
            int k = KList[i];
            Path evaluatePath = getEvaluatePath(new Path(knnDir, ""+k));
            SequenceFileDirIterator<TypeAndNWritable, DoubleWritable> it
                = open(TypeAndNWritable.class, DoubleWritable.class, evaluatePath);
            while (it.hasNext()) {
                Pair<TypeAndNWritable, DoubleWritable> pair = it.next();
                int type = pair.getFirst().getType();
                int n = pair.getFirst().getN();
                double value = pair.getSecond().get();
                if (n == N) {
                    if (type == TypeAndNWritable.TYPE_PRECISION) {
                        precisionValues[0][i] = k;
                        precisionValues[1][i] = value;
                        HadoopHelper.log(this, "k="+k+", value="+value);
                    } else if (type == TypeAndNWritable.TYPE_RECALL) {
                        recallValues[0][i] = k;
                        recallValues[1][i] = value;
                    } else if (type == TypeAndNWritable.TYPE_COVERAGE) {
                        coverageValues[0][i] = k;
                        coverageValues[1][i] = value;
                    } else if (type == TypeAndNWritable.TYPE_POPULARITY) {
                        popularityValues[0][i] = k;
                        popularityValues[1][i] = value;
                    }
                }
            }
            it.close();
        }
        
        new XYChartDrawer()
            .setXLabel("k")
            .setYLabel("precision")
            .setTitle(titlePrefix + " Precision")
            .setOutputFile("img/others/knn-precision-" + titlePrefix + ".png")
            .setPercentageFormat(true)
            .addSeries("", precisionValues)
            .setWidth(WIDTH)
            .setHeight(HEIGHT)
            .draw();
        
        new XYChartDrawer()
            .setXLabel("k")
            .setYLabel("recall")
            .setTitle(titlePrefix + " Recall")
            .setOutputFile("img/others/knn-recall-" + titlePrefix + ".png")
            .setPercentageFormat(true)
            .addSeries("", recallValues)
            .setWidth(WIDTH)
            .setHeight(HEIGHT)
            .draw();
        
        new XYChartDrawer()
            .setXLabel("k")
            .setYLabel("coverage")
            .setTitle(titlePrefix + " Coverage")
            .setOutputFile("img/others/knn-coverage-" + titlePrefix + ".png")
            .setPercentageFormat(true)
            .addSeries("", coverageValues)
            .setWidth(WIDTH)
            .setHeight(HEIGHT)
            .draw();
        
        new XYChartDrawer()
            .setXLabel("k")
            .setYLabel("popularity")
            .setTitle(titlePrefix + " Popularity")
            .setOutputFile("img/others/knn-popularity-" + titlePrefix + ".png")
            .setPercentageFormat(false)
            .addSeries("", popularityValues)
            .setWidth(WIDTH)
            .setHeight(HEIGHT)
            .draw();
    }
    
    private Path getEvaluatePath(Path knnDir) {
        return new Path(knnDir, "evaluate");
    }
    
    private Path getResultPath(Path knnDir) {
        return new Path(knnDir, "result");
    }
    
    public static void main(String[] args) {
        KNN job = new KNN();
        try {
            ToolRunner.run(job, args);
        } catch (Exception e) {
            L.e(job, e);
        }
    }
}
