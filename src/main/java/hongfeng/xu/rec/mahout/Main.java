/**
 * 2013-4-2
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout;

import hongfeng.xu.rec.mahout.chart.ChartDrawer;
import hongfeng.xu.rec.mahout.chart.Result;
import hongfeng.xu.rec.mahout.config.DataSetConfig;
import hongfeng.xu.rec.mahout.hadoop.BaseJob;
import hongfeng.xu.rec.mahout.hadoop.eval.EvaluateRecommenderJob;
import hongfeng.xu.rec.mahout.hadoop.matrix.DrawMatrixJob;
import hongfeng.xu.rec.mahout.hadoop.matrix.ToVectorJob;
import hongfeng.xu.rec.mahout.hadoop.misc.ComputeIntersectJob;
import hongfeng.xu.rec.mahout.hadoop.parser.RawDataParser;
import hongfeng.xu.rec.mahout.hadoop.recommender.ItemBasedRecommender;
import hongfeng.xu.rec.mahout.hadoop.recommender.PopularRecommender;
import hongfeng.xu.rec.mahout.hadoop.recommender.RandomRecommender;
import hongfeng.xu.rec.mahout.hadoop.recommender.UserBasedRecommender;
import hongfeng.xu.rec.mahout.hadoop.threshold.ItemThresholdRecommenderV2;
import hongfeng.xu.rec.mahout.hadoop.threshold.ThresholdRecommenderV2;
import hongfeng.xu.rec.mahout.structure.TypeAndNWritable;
import hongfeng.xu.rec.mahout.util.L;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterator;

/**
 * @author xuhongfeng
 *
 */
public class Main extends BaseJob {
    private Map<String, Result> coverageResult = new HashMap<String, Result>();
    private Map<String, Result> popularityResult = new HashMap<String, Result>();
    private Map<String, Result> precisionResult = new HashMap<String, Result>();
    private Map<String, Result> recallResult = new HashMap<String, Result>();
    
    private static final int k = 1000;

    @Override
    protected int innerRun() throws Exception {
        parseRawData();
        
        toVector();
        
//        drawIntersect();
        
        /* random recommender */
        EvaluateRecommenderJob<RandomRecommender> evaluateRandom =
                new EvaluateRecommenderJob<RandomRecommender>(new RandomRecommender(),
                DataSetConfig.getRandomRecommenderResultPath());
        runJob(evaluateRandom, DataSetConfig.getUserItemVectorPath(),
                DataSetConfig.getRandomRecommenderEvaluate(), true);
        
        /* popular recommender */
        EvaluateRecommenderJob<PopularRecommender> evaluatePopular =
                new EvaluateRecommenderJob<PopularRecommender>(new PopularRecommender(),
                DataSetConfig.getPopularRecommenderResultPath());
        runJob(evaluatePopular, DataSetConfig.getUserItemVectorPath(),
                DataSetConfig.getPopularRecommederEvaluate(), true);
        
        /* user based recommender */
        EvaluateRecommenderJob<UserBasedRecommender> evaluateUserBased =
                new EvaluateRecommenderJob<UserBasedRecommender>(new UserBasedRecommender(k),
                DataSetConfig.getUserBasedResult());
        runJob(evaluateUserBased, DataSetConfig.getUserItemVectorPath(),
                DataSetConfig.getUserBasedEvaluate(), true);
        
        calculateResult(DataSetConfig.getRandomRecommenderEvaluate(), "random");
        calculateResult(DataSetConfig.getPopularRecommederEvaluate(), "popular");
        calculateResult(DataSetConfig.getUserBasedEvaluate(), "UserBased");
        
        /* threshold recommender */
        int[] thresholdList = new int[] {0, 20,  40, 60, 80, 100, 120, 150, 200, 500};
//        int[] thresholdList = new int[] {100};
//        for (int threshold:thresholdList) {
//            EvaluateRecommenderJob<ThresholdRecommender> evaluateThreshold =
//                    new EvaluateRecommenderJob<ThresholdRecommender>(new ThresholdRecommender(threshold, k),
//                    DataSetConfig.getUserThresholdResult(threshold));
//            runJob(evaluateThreshold, DataSetConfig.getUserItemVectorPath(),
//                    DataSetConfig.getUserThresholdEvaluate(threshold), true);
//            calculateResult(DataSetConfig.getUserThresholdEvaluate(threshold), "User-Threshold-" + threshold);
//        }
        for (int threshold: thresholdList) {
            EvaluateRecommenderJob<ThresholdRecommenderV2> evaluateThreshold = new EvaluateRecommenderJob<ThresholdRecommenderV2>(
                    new ThresholdRecommenderV2(threshold, k),
                    DataSetConfig.getV2UserThresholdResult(threshold));
            runJob(evaluateThreshold, DataSetConfig.getUserItemVectorPath(),
                    DataSetConfig.getV2UserThresholdEvaluate(threshold), true);
            calculateResult(
                    DataSetConfig.getV2UserThresholdEvaluate(threshold),
                    "User-Threshold-V2-" + threshold);
        }
        
        
        /* item based recommender */
        EvaluateRecommenderJob<ItemBasedRecommender> evaluateItemBased =
                new EvaluateRecommenderJob<ItemBasedRecommender>(new ItemBasedRecommender(k),
                DataSetConfig.getItemBasedResult());
        runJob(evaluateItemBased, DataSetConfig.getUserItemVectorPath(),
                DataSetConfig.getItemBasedEvaluate(), true);
        calculateResult(DataSetConfig.getItemBasedEvaluate(), "ItemBased");
////        
////        /* threshold recommender */
//        thresholdList = new int[] {5, 10, 20, 30, 40, 50, 80, 100};
////        thresholdList = new int[] {30};
//        for (int threshold:thresholdList) {
//            EvaluateRecommenderJob<ItemThresholdRecommender> evaluateThreshold =
//                    new EvaluateRecommenderJob<ItemThresholdRecommender>(
//                            new ItemThresholdRecommender(threshold, k),
//                    DataSetConfig.getItemThresholdResult(threshold));
//            runJob(evaluateThreshold, DataSetConfig.getUserItemVectorPath(),
//                    DataSetConfig.getItemThresholdEvaluate(threshold), true);
//            calculateResult(DataSetConfig.getItemThresholdEvaluate(threshold), "Item-Threshold-" + threshold);
//        }
        for (int threshold:thresholdList) {
            EvaluateRecommenderJob<ItemThresholdRecommenderV2> evaluateThreshold =
                    new EvaluateRecommenderJob<ItemThresholdRecommenderV2>(
                            new ItemThresholdRecommenderV2(threshold, k),
                    DataSetConfig.getV2ItemThresholdResult(threshold));
            runJob(evaluateThreshold, DataSetConfig.getUserItemVectorPath(),
                    DataSetConfig.getV2ItemThresholdEvaluate(threshold), true);
            calculateResult(DataSetConfig.getV2ItemThresholdEvaluate(threshold), "Item-Threshold-v2-" + threshold);
        }
        
        ChartDrawer chartDrawer = new ChartDrawer("Coverage Rate", "coverage", "img/coverage.png", coverageResult, true);
        chartDrawer.draw();
        chartDrawer = new ChartDrawer("Precision Rate", "precision", "img/precision.png", precisionResult, true);
        chartDrawer.draw();
        chartDrawer = new ChartDrawer("Recall Rate", "recall", "img/recall.png", recallResult, true);
        chartDrawer.draw();
        chartDrawer = new ChartDrawer("Popularity", "popularity", "img/popularity.png", popularityResult, false);
        chartDrawer.draw();
        
//        for (int threshold:thresholdList) {
//            drawSimilarity(threshold);
//        }
//        
//        Path[] matrixDir = new Path[] {
//                DataSetConfig.getUserBasedMatrix(), DataSetConfig.getV2UUUIThresholdPath(10)
//        };
//        String[] series = new String[] {
//                "UserBased", "Threshold-v2"
//        };
//        DrawMatrixJob drawUI = new DrawMatrixJob(0.0001f, "img/others/ui.png", "", new String[0], matrixDir, series, false, false);
//        runJob(drawUI, new Path("test"), new Path("test"), false);
        
//        ComputeIntersectJob computeIntersectJob = new ComputeIntersectJob(userCount(),
//                itemCount(), userCount(), DataSetConfig.getUserItemVectorPath());
//        runJob(computeIntersectJob, DataSetConfig.getUserItemVectorPath(), DataSetConfig.getIntersectPath(),
//                true);
//        DrawMatrixJob drawIntersect = new DrawMatrixJob(0.1f, "img/others/intersect.png",
//                "", new String[0], new Path[] {
//                DataSetConfig.getIntersectPath()
//        }, new String[] {
//                "intersect"
//        }, false, true);
//        runJob(drawIntersect, new Path("test"), new Path("test"), false);
        
        return 0;
    }
    
    private void toVector() throws Exception {
        ToVectorJob job = new ToVectorJob();
        runJob(job, DataSetConfig.getRawDataPath(), DataSetConfig.getMatrixPath(), false);
        
        //draw distribution
//        float precesion = 0.001f;
//        String imageFile = "img/others/distribution_origin_matrix.png";
//        String title = "origin matrix distribution";
//        DrawMatrixJob drawJob = new DrawMatrixJob(precesion, imageFile, title,
//                new String[0], new Path[] {DataSetConfig.getUserItemMatrixPath()},
//                new String[] {""}, true, false);
//        runJob(drawJob, DataSetConfig.getUserItemMatrixPath(), new Path(DataSetConfig.getUserItemMatrixPath(),
//                "distributon"), false);
    }
    
    private void parseRawData() throws Exception {
        
        Path output = DataSetConfig.getRawDataPath();
        RawDataParser parser = new RawDataParser(DataSetConfig.inputAll, DataSetConfig.inputTraining,
                DataSetConfig.inputTest);
        runJob(parser, DataSetConfig.inputAll, output, false);
//        
//        new DrawRawData(DataSetConfig.inputTraining, DataSetConfig.getTrainingDataPath(),
//                "rawData-training", getConf()).draw("img/others/rawData-training.png");
//        new DrawRawData(DataSetConfig.inputTest, DataSetConfig.getTestDataPath(),
//                "rawData-test", getConf()).draw("img/others/rawData-test.png");
    }
    
    private void drawSimilarity(int threshold) throws Exception {
        float precision = 0.0001f;
        String imageFile = "img/others/similarity_distribution-" + threshold + ".png";
        String title = "similarity";
        String[] subTitles = new String[0];
        Path[] matrixDirs = new Path[] {
                DataSetConfig.getUserSimilarityPath(),
                DataSetConfig.getUserSimilarityThresholdPath(threshold),
                DataSetConfig.getV2UserAllocate(threshold),
                DataSetConfig.getV2UserMultiplyAllocate(threshold),
                DataSetConfig.getV2UserDoAllocate(threshold),
                DataSetConfig.getV2UUThresholdPath(threshold)
        };
        String[] series = new String[] {
                "origin",
                "filter-" + threshold,
                "allocate-" + threshold,
                "multiply-allocate-" + threshold,
                "do-allocate-" + threshold,
                "threshold-" + threshold
        };
        boolean withZero = true;
        boolean diagonalOnly = false;
        DrawMatrixJob drawJob = new DrawMatrixJob(precision, imageFile, title, subTitles,
                matrixDirs, series, withZero, diagonalOnly);
        runJob(drawJob, new Path("test"), new Path("test"), false);
    }
    
    private void calculateResult(Path evaluatePath, String name) throws IOException {
        SequenceFileDirIterator<TypeAndNWritable, DoubleWritable> iterator =
                new SequenceFileDirIterator<TypeAndNWritable, DoubleWritable>(
                        evaluatePath, PathType.LIST,
                        PathFilters.partFilter(), null, false, getConf());
        Result resultCoverage = new Result();
        Result resultPrecision = new Result();
        Result resultRecall = new Result();
        Result resultPopularity = new Result();
        while (iterator.hasNext()) {
            Pair<TypeAndNWritable, DoubleWritable> pair = iterator.next();
            int type = pair.getFirst().getType();
            int n = pair.getFirst().getN();
            double value = pair.getSecond().get();
            if (type == TypeAndNWritable.TYPE_COVERAGE) {
                resultCoverage.put(n, value);
            } else if (type == TypeAndNWritable.TYPE_PRECISION) {
                resultPrecision.put(n, value);
            } else if (type == TypeAndNWritable.TYPE_RECALL) {
                resultRecall.put(n, value);
            } else if (type == TypeAndNWritable.TYPE_POPULARITY) {
                resultPopularity.put(n, value);
            }
//            HadoopHelper.log(this, pair.toString());
        }
        iterator.close();
        coverageResult.put(name, resultCoverage);
        popularityResult.put(name, resultPopularity);
        precisionResult.put(name, resultPrecision);
        recallResult.put(name, resultRecall);
    }
    
    private void drawIntersect() throws Exception {
        int n1 = userCount();
        int n2 = itemCount();
        int n3 = n1;
        Path multiplyerPath = DataSetConfig.getUserItemVectorPath();
        Path input = multiplyerPath;
        Path output = DataSetConfig.getIntersectPath();
        ComputeIntersectJob computeIntersectJob = new ComputeIntersectJob(n1, n2, n3, multiplyerPath);
        runJob(computeIntersectJob, input, output, true);
        
        float precision = 1f;
        String imageFile = "img/others/intersect.png";
        String title = "intersect";
        String[] subTitles = new String[0];
        Path[] matrixDirs = new Path[] {DataSetConfig.getIntersectPath()};
        String[] series = new String[] {"count"};
        boolean withZero = false;
        boolean diagonalOnly = true;
        DrawMatrixJob drawMatrixJob = new DrawMatrixJob(precision, imageFile, title,
                subTitles, matrixDirs, series, withZero, diagonalOnly);
        runJob(drawMatrixJob, new Path("test"), new Path("test"), false);
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
