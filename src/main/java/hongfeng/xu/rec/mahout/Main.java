/**
 * 2013-4-2 xuhongfeng
 */
package hongfeng.xu.rec.mahout;

import hongfeng.xu.rec.mahout.chart.ChartDrawer;
import hongfeng.xu.rec.mahout.chart.Result;
import hongfeng.xu.rec.mahout.chart.XYChartDrawer;
import hongfeng.xu.rec.mahout.config.DataSetConfig;
import hongfeng.xu.rec.mahout.hadoop.BaseJob;
import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;
import hongfeng.xu.rec.mahout.hadoop.eval.EvaluateRecommenderJob;
import hongfeng.xu.rec.mahout.hadoop.matrix.DrawMatrixJob;
import hongfeng.xu.rec.mahout.hadoop.matrix.ToVectorJob;
import hongfeng.xu.rec.mahout.hadoop.parser.RawDataParser;
import hongfeng.xu.rec.mahout.hadoop.recommender.ItemBasedRecommender;
import hongfeng.xu.rec.mahout.hadoop.recommender.PopularRecommender;
import hongfeng.xu.rec.mahout.hadoop.recommender.RandomRecommender;
import hongfeng.xu.rec.mahout.hadoop.recommender.ThresholdV3;
import hongfeng.xu.rec.mahout.hadoop.recommender.UserBasedRecommender;
import hongfeng.xu.rec.mahout.hadoop.threshold.ItemThresholdRecommenderV2;
import hongfeng.xu.rec.mahout.hadoop.threshold.ThresholdRecommenderV2;
import hongfeng.xu.rec.mahout.hadoop.threshold.v4.ThresholdRecommenderV4;
import hongfeng.xu.rec.mahout.hadoop.threshold.v5.ThresholdRecommenderV5;
import hongfeng.xu.rec.mahout.structure.TypeAndNWritable;
import hongfeng.xu.rec.mahout.util.L;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterator;

/**
 * @author xuhongfeng
 */
public class Main extends BaseJob {
    private Map<String, Result> coverageResult = new HashMap<String, Result>();

    private Map<String, Result> popularityResult = new HashMap<String, Result>();

    private Map<String, Result> precisionResult = new HashMap<String, Result>();

    private Map<String, Result> recallResult = new HashMap<String, Result>();

    private static int k = DataSetConfig.ROOT.toString().contains("appchina")?5000:2000;
    
    private ExecutorService threadPool = Executors.newCachedThreadPool();
    private List<Future<Void>> tasks = new ArrayList<Future<Void>> ();

    private int[] thresholdList = new int[] {
//        0, 20, 40, 60, 80, 100, 120, 150, 200, 500
//        0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 40, 60
//        0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 40, 60
//        0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20
//          16, 40
//            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
//            30, 40, 50, 60, 70, 80, 90, 100
//          0, 2, 4, 6, 8, 10
//            0, 1, 2, 3, 4, 5, 6, 8, 10, 15, 20, 30, 35, 40, 45, 50 
//            6
            3
    };

    @Override
    protected int innerRun() throws Exception {
        parseRawData();
//
        toVector();

        evaluate();
        
        drawThreshold();
//        
//        drawItemSimilarity();
        
//        drawUserSimilarity();
//        
//        drawUserBasedUI();
        
//        float precision = 0.0001f;
//        String imageFile = "img/others/item-similarity.png";
//        String title = "Item Similarity";
//        String[] subTitles = new String[0];
//        Path[] matrixDirs = new Path[] {
//            DataSetConfig.getItemSimilarityPath(),
//        };
//        String[] series = new String[] {
//            "ItemSimilarity"
//        };
//        boolean withZero = true;
//        boolean diagonalOnly = false;
//        DrawMatrixJob drawJob = new DrawMatrixJob(precision, imageFile, title,
//                subTitles, matrixDirs, series, withZero, diagonalOnly);
//        runJob(drawJob, new Path("test"), new Path("test"), false);
//        
        return 0;
    }
    
    private void waitForTasks() throws InterruptedException, ExecutionException {
        for (Future<Void> future:tasks) {
            future.get();
        }
        tasks.clear();
    }
    
    private void drawThreshold() throws IOException {
        int COUNT = 1;
        double[][][] coverageUser = new double[COUNT][2][thresholdList.length];
        double[][][] coverageItem = new double[COUNT][2][thresholdList.length];
        double[][][] recallUser = new double[COUNT][2][thresholdList.length];
        double[][][] recallItem = new double[COUNT][2][thresholdList.length];
        double[][][] precesionUser = new double[COUNT][2][thresholdList.length];
        double[][][] precesionItem = new double[COUNT][2][thresholdList.length];
        double[][][] popularityUser = new double[COUNT][2][thresholdList.length];
        double[][][] popularityItem = new double[COUNT][2][thresholdList.length];
        
        for (int i=0; i<thresholdList.length; i++) {
            int threshold = thresholdList[i];
            SequenceFileDirIterator<TypeAndNWritable, DoubleWritable> it
                = open(TypeAndNWritable.class, DoubleWritable.class,
                        DataSetConfig.getV2UserThresholdEvaluate(threshold));
            while (it.hasNext()) {
                Pair<TypeAndNWritable, DoubleWritable> pair = it.next();
                int type = pair.getFirst().getType();
                int n = pair.getFirst().getN();
                int idx = n/10 - 1;
                if (idx >= COUNT) {
                    continue;
                }
                double value = pair.getSecond().get();
                switch (type) {
                    case TypeAndNWritable.TYPE_RECALL:
                        recallUser[idx][0][i] = threshold;
                        recallUser[idx][1][i] = value;
                        HadoopHelper.log(this, String.format("Recall : threshold=%d, n=%d, value=%.3f"
                                , threshold, n, value));
                        break;
                    case TypeAndNWritable.TYPE_PRECISION:
                        precesionUser[idx][0][i] = threshold;
                        precesionUser[idx][1][i] = value;
                        HadoopHelper.log(this, String.format("Precision : threshold=%d, n=%d, value=%.3f"
                                , threshold, n, value));
                        break;
                    case TypeAndNWritable.TYPE_POPULARITY:
                        popularityUser[idx][0][i] = threshold;
                        popularityUser[idx][1][i] = value;
                        HadoopHelper.log(this, String.format("Popularity : threshold=%d, n=%d, value=%.3f"
                                , threshold, n, value));
                        break;
                    case TypeAndNWritable.TYPE_COVERAGE:
                        coverageUser[idx][0][i] = threshold;
                        coverageUser[idx][1][i] = value;
                        HadoopHelper.log(this, String.format("Coverage: threshold=%d, n=%d, value=%.3f"
                                , threshold, n, value));
                        break;
                }
            }
            it.close();
        }
        
        for (int i=0; i<thresholdList.length; i++) {
            int threshold = thresholdList[i];
            SequenceFileDirIterator<TypeAndNWritable, DoubleWritable> it
                = open(TypeAndNWritable.class, DoubleWritable.class,
                        DataSetConfig.getV2ItemThresholdEvaluate(threshold));
            while (it.hasNext()) {
                Pair<TypeAndNWritable, DoubleWritable> pair = it.next();
                int type = pair.getFirst().getType();
                int n = pair.getFirst().getN();
                int idx = n/10 - 1;
                if (idx >= COUNT) {
                    continue;
                }
                double value = pair.getSecond().get();
                switch (type) {
                    case TypeAndNWritable.TYPE_RECALL:
                        recallItem[idx][0][i] = threshold;
                        recallItem[idx][1][i] = value;
                        break;
                    case TypeAndNWritable.TYPE_PRECISION:
                        precesionItem[idx][0][i] = threshold;
                        precesionItem[idx][1][i] = value;
                        break;
                    case TypeAndNWritable.TYPE_POPULARITY:
                        popularityItem[idx][0][i] = threshold;
                        popularityItem[idx][1][i] = value;
                        break;
                    case TypeAndNWritable.TYPE_COVERAGE:
                        coverageItem[idx][0][i] = threshold;
                        coverageItem[idx][1][i] = value;
                        break;
                }
            }
            it.close();
        }
        
        XYChartDrawer drawer = new XYChartDrawer();
        drawer.setTitle("Recall");
        drawer.setXLabel("threshold");
        drawer.setYLabel("value");
        for (int i=0; i<recallUser.length; i++) {
            drawer.addSeries("TOP-" + (i+1)*10, recallUser[i]);
        }
        drawer.setOutputFile("img/threshold/userBased/recall.png");
        drawer.setPercentageFormat(true);
        drawer.draw();
        
        drawer = new XYChartDrawer();
        drawer.setTitle("Precision");
        drawer.setXLabel("threshold");
        drawer.setYLabel("value");
        for (int i=0; i<recallUser.length; i++) {
            drawer.addSeries("TOP-" + (i+1)*10, precesionUser[i]);
        }
        drawer.setOutputFile("img/threshold/userBased/precision.png");
        drawer.setPercentageFormat(true);
        drawer.draw();
        
        drawer = new XYChartDrawer();
        drawer.setTitle("Coverage");
        drawer.setXLabel("threshold");
        drawer.setYLabel("value");
        for (int i=0; i<recallUser.length; i++) {
            drawer.addSeries("TOP-" + (i+1)*10, coverageUser[i]);
        }
        drawer.setOutputFile("img/threshold/userBased/coverage.png");
        drawer.setPercentageFormat(true);
        drawer.draw();
        
        drawer = new XYChartDrawer();
        drawer.setTitle("Popular");
        drawer.setXLabel("threshold");
        drawer.setYLabel("value");
        for (int i=0; i<recallUser.length; i++) {
            drawer.addSeries("TOP-" + (i+1)*10, popularityUser[i]);
        }
        drawer.setOutputFile("img/threshold/userBased/popularity.png");
        drawer.setPercentageFormat(false);
        drawer.draw();
        
        /* ItemBased */
        drawer = new XYChartDrawer();
        drawer.setTitle("Recall");
        drawer.setXLabel("threshold");
        drawer.setYLabel("value");
        for (int i=0; i<recallItem.length; i++) {
            drawer.addSeries("TOP-" + (i+1)*10, recallItem[i]);
        }
        drawer.setOutputFile("img/threshold/itemBased/recall.png");
        drawer.setPercentageFormat(true);
        drawer.draw();
        
        drawer = new XYChartDrawer();
        drawer.setTitle("Precision");
        drawer.setXLabel("threshold");
        drawer.setYLabel("value");
        for (int i=0; i<recallItem.length; i++) {
            drawer.addSeries("TOP-" + (i+1)*10, precesionItem[i]);
        }
        drawer.setOutputFile("img/threshold/itemBased/precision.png");
        drawer.setPercentageFormat(true);
        drawer.draw();
        
        drawer = new XYChartDrawer();
        drawer.setTitle("Coverage");
        drawer.setXLabel("threshold");
        drawer.setYLabel("value");
        for (int i=0; i<recallItem.length; i++) {
            drawer.addSeries("TOP-" + (i+1)*10, coverageItem[i]);
        }
        drawer.setOutputFile("img/threshold/itemBased/coverage.png");
        drawer.setPercentageFormat(true);
        drawer.draw();
        
        drawer = new XYChartDrawer();
        drawer.setTitle("Popular");
        drawer.setXLabel("threshold");
        drawer.setYLabel("value");
        for (int i=0; i<recallItem.length; i++) {
            drawer.addSeries("TOP-" + (i+1)*10, popularityItem[i]);
        }
        drawer.setOutputFile("img/threshold/itemBased/popularity.png");
        drawer.setPercentageFormat(false);
        drawer.draw();
    }

//    private void drawUserBasedUI() throws Exception {
//        Path[] matrixDir = new Path[] {
//            DataSetConfig.getUserBasedMatrix(k),
//            DataSetConfig.getV2UUUIThresholdPath(14)
//        };
//        String[] series = new String[] {
//            "UserBased", "Threshold-v2"
//        };
//        DrawMatrixJob drawUI = new DrawMatrixJob(0.0001f, "img/others/ui.png",
//                "", new String[0], matrixDir, series, true, false);
//        runJob(drawUI, new Path("test"), new Path("test"), false);
//
//    }

    private void evaluateRandom() throws Exception {
        /* random recommender */
        EvaluateRecommenderJob<RandomRecommender> evaluateRandom = new EvaluateRecommenderJob<RandomRecommender>(
                new RandomRecommender(),
                DataSetConfig.getRandomRecommenderResultPath());
        runJob(evaluateRandom, DataSetConfig.getUserItemVectorPath(),
                DataSetConfig.getRandomRecommenderEvaluate(), true);
        calculateResult(DataSetConfig.getRandomRecommenderEvaluate(), "random");
    }

    private void evaluateItembasedV2() throws Exception {
        // for (int threshold: thresholdList) {
        // EvaluateRecommenderJob<ItemThresholdRecommender> evaluateThreshold =
        // new EvaluateRecommenderJob<ItemThresholdRecommender>(
        // new ItemThresholdRecommender(threshold, k),
        // DataSetConfig.getItemThresholdResult(threshold));
        // runJob(evaluateThreshold, DataSetConfig.getUserItemVectorPath(),
        // DataSetConfig.getItemThresholdEvaluate(threshold), true);
        // calculateResult(DataSetConfig.getItemThresholdEvaluate(threshold),
        // "Item-Threshold-" + threshold);
        // }
        for (int threshold: thresholdList) {
            EvaluateRecommenderJob<ItemThresholdRecommenderV2> evaluateThreshold = new EvaluateRecommenderJob<ItemThresholdRecommenderV2>(
                    new ItemThresholdRecommenderV2(threshold, k),
                    DataSetConfig.getV2ItemThresholdResult(threshold));
            runJob(evaluateThreshold, DataSetConfig.getUserItemVectorPath(),
                    DataSetConfig.getV2ItemThresholdEvaluate(threshold), true);
            calculateResult(
                    DataSetConfig.getV2ItemThresholdEvaluate(threshold),
                    "Item-Threshold-v2-" + threshold);
        }
    }

    private void evaluateItembased() throws Exception {
        /* item based recommender */
        EvaluateRecommenderJob<ItemBasedRecommender> evaluateItemBased = new EvaluateRecommenderJob<ItemBasedRecommender>(
                new ItemBasedRecommender(k), DataSetConfig.getItemBasedResult());
        runJob(evaluateItemBased, DataSetConfig.getUserItemVectorPath(),
                DataSetConfig.getItemBasedEvaluate(), true);
        calculateResult(DataSetConfig.getItemBasedEvaluate(), "ItemBased");
    }

    private void evaluateUserbasedV3() throws Exception {
        for (int threshold: thresholdList) {
            EvaluateRecommenderJob<ThresholdV3> job = new EvaluateRecommenderJob<ThresholdV3>
                (new ThresholdV3(threshold, k), DataSetConfig.getV3Result(threshold));
            runJob(job, DataSetConfig.getUserItemVectorPath(), DataSetConfig.getV3Evaluate(threshold), true);
            calculateResult(DataSetConfig.getV3Evaluate(threshold), "V3-" + threshold);
        }
    }

    private void evaluateUserbasedV2() throws Exception {
        for (final int threshold: thresholdList) {
            Future<Void> future = threadPool.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    EvaluateRecommenderJob<ThresholdRecommenderV2> evaluateThreshold = new EvaluateRecommenderJob<ThresholdRecommenderV2>(
                            new ThresholdRecommenderV2(threshold, k),
                            DataSetConfig.getV2UserThresholdResult(threshold));
                    try {
                        String title = "ImprovedUserBased";
                        if (thresholdList.length > 1) {
                            title = title + "-" + threshold;
                        }
                        runJob(evaluateThreshold, DataSetConfig.getUserItemVectorPath(),
                                DataSetConfig.getV2UserThresholdEvaluate(threshold), true);
                        calculateResult(
                                DataSetConfig.getV2UserThresholdEvaluate(threshold),
                                title);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return null;
                }});
            tasks.add(future);
        }
    }

    private void evaluateUserbasedV5() throws Exception {
        for (int threshold: thresholdList) {
            EvaluateRecommenderJob<ThresholdRecommenderV5> evaluateThreshold = new EvaluateRecommenderJob<ThresholdRecommenderV5>(
                    new ThresholdRecommenderV5(threshold, k),
                    DataSetConfig.getV5Result(threshold));
            runJob(evaluateThreshold, DataSetConfig.getUserItemVectorPath(),
                    DataSetConfig.getV5Evaluate(threshold), true);
            calculateResult(
                    DataSetConfig.getV5Evaluate(threshold),
                    "V5-" + threshold);
        }
    }


    private void evaluateUserbasedV4() throws Exception {
        int bottom = 6;
        int[] topList = new int[] {55, 60, 65, 70, 75, 80, 85, 90, 95};
        for (int top:topList) {
            EvaluateRecommenderJob<ThresholdRecommenderV4> evaluateThreshold = new EvaluateRecommenderJob<ThresholdRecommenderV4>(
                    new ThresholdRecommenderV4(bottom, top, k),
                    DataSetConfig.getV4ResultPath(bottom, top));
            runJob(evaluateThreshold, DataSetConfig.getUserItemVectorPath(),
                    DataSetConfig.getV4EvaluatePath(bottom, top), true);
            calculateResult(
                    DataSetConfig.getV4EvaluatePath(bottom, top),
                    "V4-"+top);
        }
    }

    private void evaluateUserbased() throws Exception {
        /* user based recommender */
        EvaluateRecommenderJob<UserBasedRecommender> evaluateUserBased = new EvaluateRecommenderJob<UserBasedRecommender>(
                new UserBasedRecommender(k), DataSetConfig.getUserBasedResult());
        runJob(evaluateUserBased, DataSetConfig.getUserItemVectorPath(),
                DataSetConfig.getUserBasedEvaluate(), true);

        calculateResult(DataSetConfig.getUserBasedEvaluate(), "UserBased");
    }

    private void evaluatePopular() throws Exception {
        /* popular recommender */
        EvaluateRecommenderJob<PopularRecommender> evaluatePopular = new EvaluateRecommenderJob<PopularRecommender>(
                new PopularRecommender(),
                DataSetConfig.getPopularRecommenderResultPath());
        runJob(evaluatePopular, DataSetConfig.getUserItemVectorPath(),
                DataSetConfig.getPopularRecommederEvaluate(), true);
        calculateResult(DataSetConfig.getPopularRecommederEvaluate(), "popular");
    }

    private void toVector() throws Exception {
        ToVectorJob job = new ToVectorJob();
        runJob(job, DataSetConfig.getRawDataPath(),
                DataSetConfig.getMatrixPath(), false);

        // draw distribution
        // float precesion = 0.001f;
        // String imageFile = "img/others/distribution_origin_matrix.png";
        // String title = "origin matrix distribution";
        // DrawMatrixJob drawJob = new DrawMatrixJob(precesion, imageFile,
        // title,
        // new String[0], new Path[] {DataSetConfig.getUserItemMatrixPath()},
        // new String[] {""}, true, false);
        // runJob(drawJob, DataSetConfig.getUserItemMatrixPath(), new
        // Path(DataSetConfig.getUserItemMatrixPath(),
        // "distributon"), false);
    }

    private void parseRawData() throws Exception {

        Path output = DataSetConfig.getRawDataPath();
        RawDataParser parser = new RawDataParser(DataSetConfig.inputAll,
                DataSetConfig.inputTraining, DataSetConfig.inputTest);
        runJob(parser, DataSetConfig.inputAll, output, false);
        //
        // new DrawRawData(DataSetConfig.inputTraining,
        // DataSetConfig.getTrainingDataPath(),
        // "rawData-training",
        // getConf()).draw("img/others/rawData-training.png");
        // new DrawRawData(DataSetConfig.inputTest,
        // DataSetConfig.getTestDataPath(),
        // "rawData-test", getConf()).draw("img/others/rawData-test.png");
    }

    private void drawItemSimilarity() throws Exception {
        for (int threshold: thresholdList) {
            drawItemSimilarity(threshold);
        }
    }

    private void drawUserSimilarity() throws Exception {
        for (int threshold: thresholdList) {
            drawUserSimilarity(threshold);
        }
    }

    private void drawItemSimilarity(int threshold) throws Exception {
        float precision = 0.0001f;
        String imageFile = "img/others/item-similarity-" + threshold
                + ".png";
        String title = "similarity";
        String[] subTitles = new String[0];
        Path[] matrixDirs = new Path[] {
            DataSetConfig.getItemSimilarityPath(),
            DataSetConfig.getItemSimilarityThresholdPath(threshold),
            DataSetConfig.getV2ItemAllocate(threshold),
            DataSetConfig.getV2ItemMultiplyAllocate(threshold),
            DataSetConfig.getV2ItemDoAllocate(threshold),
            DataSetConfig.getV2IIThresholdPath(threshold)
        };
        String[] series = new String[] {
            "origin", "filter-" + threshold, "allocate-" + threshold,
            "multiply-allocate-" + threshold, "do-allocate-" + threshold,
            "threshold-" + threshold
        };
        boolean withZero = true;
        boolean diagonalOnly = false;
        DrawMatrixJob drawJob = new DrawMatrixJob(precision, imageFile, title,
                subTitles, matrixDirs, series, withZero, diagonalOnly);
        runJob(drawJob, new Path("test"), new Path("test"), false);
    }

    private void drawUserSimilarity(int threshold) throws Exception {
        float precision = 0.0001f;
        String imageFile = "img/others/user-similarity-" + threshold
                + ".png";
        String title = "similarity";
        String[] subTitles = new String[0];
        Path[] matrixDirs = new Path[] {
//            DataSetConfig.getUserSimilarityPath(),
//            DataSetConfig.getUserSimilarityThresholdPath(threshold),
//            DataSetConfig.getV2UserAllocate(threshold),
//            DataSetConfig.getV2UserMultiplyAllocate(threshold),
            DataSetConfig.getV2UserDoAllocate(threshold),
//            DataSetConfig.getV2UUThresholdPath(threshold)
        };
        String[] series = new String[] {
//            "origin", "filter-" + threshold, "allocate-" + threshold,
//            "multiply-allocate-" + threshold,
            "do-allocate-" + threshold,
//            "threshold-" + threshold
        };
        boolean withZero = true;
        boolean diagonalOnly = false;
        DrawMatrixJob drawJob = new DrawMatrixJob(precision, imageFile, title,
                subTitles, matrixDirs, series, withZero, diagonalOnly);
        runJob(drawJob, new Path("test"), new Path("test"), false);
    }

    private void calculateResult(Path evaluatePath, String name)
            throws IOException {
        SequenceFileDirIterator<TypeAndNWritable, DoubleWritable> iterator = new SequenceFileDirIterator<TypeAndNWritable, DoubleWritable>(
                evaluatePath, PathType.LIST, PathFilters.partFilter(), null,
                false, getConf());
        Result resultCoverage = new Result();
        Result resultPrecision = new Result();
        Result resultRecall = new Result();
        Result resultPopularity = new Result();
        while (iterator.hasNext()) {
            Pair<TypeAndNWritable, DoubleWritable> pair = iterator.next();
            int type = pair.getFirst().getType();
            int n = pair.getFirst().getN();
            double value = pair.getSecond().get();
            String typeStr = "";
            String valueStr = "";
            if (type == TypeAndNWritable.TYPE_COVERAGE) {
                typeStr = "Coverage";
                resultCoverage.put(n, value);
                valueStr = String.format("%.1f%%",value*100);
            } else if (type == TypeAndNWritable.TYPE_PRECISION) {
                resultPrecision.put(n, value);
                typeStr = "Precision";
                valueStr = String.format("%.1f%%",value*100);
            } else if (type == TypeAndNWritable.TYPE_RECALL) {
                resultRecall.put(n, value);
                typeStr = "Recall";
                valueStr = String.format("%.1f%%",value*100);
            } else if (type == TypeAndNWritable.TYPE_POPULARITY) {
                typeStr = "Popularity";
                resultPopularity.put(n, value);
                valueStr = String.format("%.2f",value);
            }
            HadoopHelper.log(this, String.format("%s[type=%s; n=%d; value=%s]",
                    name, typeStr, n, valueStr));
        }
        iterator.close();
        coverageResult.put(name, resultCoverage);
        popularityResult.put(name, resultPopularity);
        precisionResult.put(name, resultPrecision);
        recallResult.put(name, resultRecall);
    }
    
    private void evaluate() throws Exception {
//        evaluateRandom();
//        evaluatePopular();
//        evaluateItembased();
//        evaluateItembasedV2();
        evaluateUserbased();
        evaluateUserbasedV2();
//        evaluateUserbasedV3();
//        evaluateUserbasedV4();
//        evaluateUserbasedV5();
        
        waitForTasks();
        
        ChartDrawer chartDrawer = new ChartDrawer("Coverage Rate", "coverage",
                "img/coverage.png", coverageResult, true);
        chartDrawer.draw();
        chartDrawer = new ChartDrawer("Precision Rate", "precision",
                "img/precision.png", precisionResult, true);
        chartDrawer.draw();
        chartDrawer = new ChartDrawer("Recall Rate", "recall",
                "img/recall.png", recallResult, true);
        chartDrawer.draw();
        chartDrawer = new ChartDrawer("Popularity", "popularity",
                "img/popularity.png", popularityResult, false);
        chartDrawer.draw();
    }

    public static void main(String[] args) {
        Main job = new Main();
        try {
            ToolRunner.run(job, args);
        } catch (Exception e) {
            L.e(job, e);
        }
    }
}
