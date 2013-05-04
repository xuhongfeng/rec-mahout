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
import hongfeng.xu.rec.mahout.hadoop.threshold.ItemThresholdRecommenderV2;
import hongfeng.xu.rec.mahout.hadoop.threshold.ThresholdRecommenderV2;
import hongfeng.xu.rec.mahout.structure.TypeAndNWritable;
import hongfeng.xu.rec.mahout.util.L;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.HadoopUtil;
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
    
    private ExecutorService threadPool = Executors.newCachedThreadPool();
    private List<Future<Void>> tasks = new ArrayList<Future<Void>> ();

    @Override
    protected int innerRun() throws Exception {
        
        int itemCount = HadoopUtil.readInt(DataSetConfig.getItemCountPath(), getConf());
        int userCount = HadoopUtil.readInt(DataSetConfig.getUserCountPath(), getConf());
        HadoopHelper.log(this, "itemCount=" + itemCount);
        HadoopHelper.log(this, "userCount=" + userCount);
        evaluateUserBased();
//        evaluateItemBased();
        
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
        int threshold = 0;
        for (int k:KList) {
            Path knnDir = DataSetConfig.getKnnItemBasedV2(k);
            Path resultPath = getResultPath(knnDir);
            Path evaluatePath = getEvaluatePath(knnDir);
            ItemThresholdRecommenderV2 recommender = new ItemThresholdRecommenderV2(threshold, k);
            EvaluateRecommenderJob<ItemThresholdRecommenderV2> job =
                    new EvaluateRecommenderJob<ItemThresholdRecommenderV2> (recommender,
                            resultPath);
            runJob(job, DataSetConfig.getUserItemVectorPath(), evaluatePath, true);
        }
        
        double[][][] itemBasedResult = parseResult(DataSetConfig.getKnnItemBasedDir(), KList);
        double[][][] itemBasedV2Result = parseResult(DataSetConfig.getKnnItemBasedV2Dir(), KList);
        
        new XYChartDrawer()
            .setXLabel("k")
            .setYLabel("precision")
            .setTitle("Precision")
            .setOutputFile("img/others/knn-precision-itemBased.png")
            .setPercentageFormat(true)
            .addSeries("itemBased", itemBasedResult[0])
            .addSeries("itemBasedV2", itemBasedV2Result[0])
            .setWidth(WIDTH)
            .setHeight(HEIGHT)
            .draw();
        
        new XYChartDrawer()
            .setXLabel("k")
            .setYLabel("recall")
            .setTitle("Recall")
            .setOutputFile("img/others/knn-recall-itemBased.png")
            .setPercentageFormat(true)
            .addSeries("itemBased", itemBasedResult[1])
            .addSeries("itemBasedV2", itemBasedV2Result[1])
            .setWidth(WIDTH)
            .setHeight(HEIGHT)
            .draw();
        
        new XYChartDrawer()
            .setXLabel("k")
            .setYLabel("coverage")
            .setTitle("Coverage")
            .setOutputFile("img/others/knn-coverage-itemBased.png")
            .setPercentageFormat(true)
            .addSeries("itemBased", itemBasedResult[2])
            .addSeries("itemBasedV2", itemBasedV2Result[2])
            .setWidth(WIDTH)
            .setHeight(HEIGHT)
            .draw();
        
        new XYChartDrawer()
            .setXLabel("k")
            .setYLabel("popularity")
            .setTitle("Popularity")
            .setOutputFile("img/others/knn-popularity-itemBased.png")
            .setPercentageFormat(false)
            .addSeries("itemBased", itemBasedResult[3])
            .addSeries("itemBasedV2", itemBasedV2Result[3])
            .setWidth(WIDTH)
            .setHeight(HEIGHT)
            .draw();
    }
    
    private void evaluateUserBased() throws Exception {
        int[] KList = new int[] {10, 20, 30, 40, 50, 60, 70, 80, 90, 100,
                150, 200, 250, 300, 400, 500, 600, 700, 800, 900};
//        int[] KList = new int[] {10, 50, 100, 150, 200, 300, 500, 900};
//        int[] KList = new int[] {100, 2000};
        for (final int k:KList) {
            Future<Void> future = threadPool.submit(new Callable<Void>() {
                @Override
                public Void call() {
                    Path knnDir = DataSetConfig.getKnnUserBased(k);
                    Path resultPath = getResultPath(knnDir);
                    Path evaluatePath = getEvaluatePath(knnDir);
                    UserBasedRecommender recommender = new UserBasedRecommender(k);
                    EvaluateRecommenderJob<UserBasedRecommender> job =
                            new EvaluateRecommenderJob<UserBasedRecommender> (recommender,
                                    resultPath);
                    try {
                        runJob(job, DataSetConfig.getUserItemVectorPath(), evaluatePath, true);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return null;
                }
            });
            tasks.add(future);
        }
        
        int t = 6;
        if (DataSetConfig.ROOT.toString().contains("appchina")) {
            t = 3;
        }
        final int threshold = t;
        for (final int k:KList) {
            Future<Void> future = threadPool.submit(new Callable<Void>() {
                @Override
                public Void call() {
                    Path knnDir = DataSetConfig.getKnnUserBasedV2(k);
                    Path resultPath = getResultPath(knnDir);
                    Path evaluatePath = getEvaluatePath(knnDir);
                    ThresholdRecommenderV2 recommender = new ThresholdRecommenderV2(threshold, k);
                    EvaluateRecommenderJob<ThresholdRecommenderV2> job =
                            new EvaluateRecommenderJob<ThresholdRecommenderV2> (recommender,
                                    resultPath);
                    try {
                        runJob(job, DataSetConfig.getUserItemVectorPath(), evaluatePath, true);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return null;
                }
            });
            tasks.add(future);
        }
//        for (int k:KList) {
//            Path knnDir = DataSetConfig.getKnnUserBasedV3(k);
//            Path resultPath = getResultPath(knnDir);
//            Path evaluatePath = getEvaluatePath(knnDir);
//            ThresholdV3 recommender = new ThresholdV3(threshold, k);
//            EvaluateRecommenderJob<ThresholdV3> job =
//                    new EvaluateRecommenderJob<ThresholdV3> (recommender,
//                            resultPath);
//            runJob(job, DataSetConfig.getUserItemVectorPath(), evaluatePath, true);
//        }
//        for (int k:KList) {
//            Path knnDir = DataSetConfig.getKnnUserBasedV5(k);
//            Path resultPath = getResultPath(knnDir);
//            Path evaluatePath = getEvaluatePath(knnDir);
//            ThresholdRecommenderV5 recommender = new ThresholdRecommenderV5(threshold, k);
//            EvaluateRecommenderJob<ThresholdRecommenderV5> job =
//                    new EvaluateRecommenderJob<ThresholdRecommenderV5> (recommender,
//                            resultPath);
//            runJob(job, DataSetConfig.getUserItemVectorPath(), evaluatePath, true);
//        }
        
        waitForTasks();
        
        double[][][] userBasedResult = parseResult(DataSetConfig.getKnnUserBasedDir(), KList);
        double[][][] userBasedV2Result = parseResult(DataSetConfig.getKnnUserBasedV2Dir(), KList);
//        double[][][] userBasedV3Result = parseResult(DataSetConfig.getKnnUserBasedV3Dir(), KList);
//        double[][][] userBasedV5Result = parseResult(DataSetConfig.getKnnUserBasedV5Dir(), KList);
        
        new XYChartDrawer()
            .setXLabel("k")
            .setYLabel("precision")
            .setTitle("Precision")
            .setOutputFile("img/others/knn-precision-userBased.png")
            .setPercentageFormat(true)
            .addSeries("userBased", userBasedResult[0])
            .addSeries("userBasedV2", userBasedV2Result[0])
//            .addSeries("userBasedV3", userBasedV3Result[0])
//            .addSeries("userBasedV5", userBasedV5Result[0])
            .setWidth(WIDTH)
            .setHeight(HEIGHT)
            .draw();
        
        new XYChartDrawer()
            .setXLabel("k")
            .setYLabel("recall")
            .setTitle("Recall")
            .setOutputFile("img/others/knn-recall-userBased.png")
            .setPercentageFormat(true)
            .addSeries("userBased", userBasedResult[1])
            .addSeries("userBasedV2", userBasedV2Result[1])
//            .addSeries("userBasedV3", userBasedV3Result[1])
//            .addSeries("userBasedV5", userBasedV5Result[1])
            .setWidth(WIDTH)
            .setHeight(HEIGHT)
            .draw();
        
        new XYChartDrawer()
            .setXLabel("k")
            .setYLabel("coverage")
            .setTitle("Coverage")
            .setOutputFile("img/others/knn-coverage-userBased.png")
            .setPercentageFormat(true)
            .addSeries("userBased", userBasedResult[2])
            .addSeries("userBasedV2", userBasedV2Result[2])
//            .addSeries("userBasedV3", userBasedV3Result[2])
//            .addSeries("userBasedV5", userBasedV5Result[2])
            .setWidth(WIDTH)
            .setHeight(HEIGHT)
            .draw();
        
        new XYChartDrawer()
            .setXLabel("k")
            .setYLabel("popularity")
            .setTitle("Popularity")
            .setOutputFile("img/others/knn-popularity-userBased.png")
            .setPercentageFormat(false)
            .addSeries("userBased", userBasedResult[3])
            .addSeries("userBasedV2", userBasedV2Result[3])
//            .addSeries("userBasedV3", userBasedV3Result[3])
//            .addSeries("userBasedV5", userBasedV5Result[3])
            .setWidth(WIDTH)
            .setHeight(HEIGHT)
            .draw();
    }
    
    private double[][][] parseResult(Path knnDir, int[] KList) throws IOException {
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
                        HadoopHelper.log(this, "Precision : k="+k+", value="+value);
                    } else if (type == TypeAndNWritable.TYPE_RECALL) {
                        recallValues[0][i] = k;
                        recallValues[1][i] = value;
                        HadoopHelper.log(this, "Recall : k="+k+", value="+value);
                    } else if (type == TypeAndNWritable.TYPE_COVERAGE) {
                        coverageValues[0][i] = k;
                        coverageValues[1][i] = value;
                        HadoopHelper.log(this, "Coverage : k="+k+", value="+value);
                    } else if (type == TypeAndNWritable.TYPE_POPULARITY) {
                        popularityValues[0][i] = k;
                        popularityValues[1][i] = value;
                        HadoopHelper.log(this, "Popularity : k="+k+", value="+value);
                    }
                }
            }
            it.close();
        }
        
        return new double[][][] {
                precisionValues,
                recallValues,
                coverageValues,
                popularityValues
        };
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
    
    private void waitForTasks() throws InterruptedException, ExecutionException {
        for (Future<Void> future:tasks) {
            future.get();
        }
        tasks.clear();
    }
}
