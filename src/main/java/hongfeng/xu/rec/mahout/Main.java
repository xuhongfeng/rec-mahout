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
import hongfeng.xu.rec.mahout.hadoop.matrix.MultiplyMatrixAverageJob;
import hongfeng.xu.rec.mahout.hadoop.matrix.ToVectorJob;
import hongfeng.xu.rec.mahout.hadoop.parser.RawDataParser;
import hongfeng.xu.rec.mahout.hadoop.recommender.PopularRecommender;
import hongfeng.xu.rec.mahout.hadoop.recommender.RandomRecommender;
import hongfeng.xu.rec.mahout.hadoop.recommender.UserBasedRecommender;
import hongfeng.xu.rec.mahout.hadoop.similarity.CosineSimilarityJob;
import hongfeng.xu.rec.mahout.hadoop.similarity.ThresholdCosineSimilarityJob;
import hongfeng.xu.rec.mahout.hadoop.threshold.MultiplyThresholdMatrixJob;
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

    @Override
    protected int innerRun() throws Exception {
        parseRawData();
        
        toVector();
        
        calculateSimilarity();
        
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
                new EvaluateRecommenderJob<UserBasedRecommender>(new UserBasedRecommender(),
                DataSetConfig.getUserBasedResult());
        runJob(evaluateUserBased, DataSetConfig.getUserItemVectorPath(),
                DataSetConfig.getUserBasedEvaluate(), true);
        
        calculateResult(DataSetConfig.getRandomRecommenderEvaluate(), "random");
        calculateResult(DataSetConfig.getPopularRecommederEvaluate(), "popular");
        calculateResult(DataSetConfig.getUserBasedEvaluate(), "UserBased");
        
        ChartDrawer chartDrawer = new ChartDrawer("Coverage Rate", "coverage", "img/coverage.png", coverageResult, true);
        chartDrawer.draw();
        chartDrawer = new ChartDrawer("Precision Rate", "precision", "img/precision.png", precisionResult, true);
        chartDrawer.draw();
        chartDrawer = new ChartDrawer("Recall Rate", "recall", "img/recall.png", recallResult, true);
        chartDrawer.draw();
        chartDrawer = new ChartDrawer("Popularity", "popularity", "img/popularity.png", popularityResult, false);
        chartDrawer.draw();
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
        
//        new DrawRawData(DataSetConfig.inputTraining, DataSetConfig.getTrainingDataPath(),
//                "rawData-training", getConf()).draw("img/others/rawData-training.png");
//        new DrawRawData(DataSetConfig.inputTest, DataSetConfig.getTestDataPath(),
//                "rawData-test", getConf()).draw("img/others/rawData-test.png");
    }
    
    private void calculateSimilarity() throws Exception {
        int n1 = userCount();
        int n2 = itemCount();
        int n3 = n1;
        int threshold = 30;
        Path multiplyerPath = DataSetConfig.getUserItemVectorPath();
        ThresholdCosineSimilarityJob thresholdCosineSimilarityJob =
                new ThresholdCosineSimilarityJob(n1, n2, n3, multiplyerPath, threshold);
        runJob(thresholdCosineSimilarityJob, DataSetConfig.getUserItemVectorPath(),
                DataSetConfig.getSimilarityThresholdPath(threshold), true);
        
        CosineSimilarityJob cosineSimilarityJob = new CosineSimilarityJob(n1, n2, n3, multiplyerPath);
        runJob(cosineSimilarityJob, DataSetConfig.getUserItemVectorPath(),
                DataSetConfig.getSimilarityPath(), true);
        
        Path similarityVectorPath = new Path(DataSetConfig.getSimilarityThresholdPath(threshold), "rowVector");
        MultiplyMatrixAverageJob matrixAverageJob = new MultiplyMatrixAverageJob(n1, n2, n3,
                similarityVectorPath);
        runJob(matrixAverageJob, similarityVectorPath, DataSetConfig.getSimilarityThresholdAveragePath(threshold)
                , true);
        
        Path averageSimilarityPath = new Path(DataSetConfig.getSimilarityThresholdAveragePath(threshold), "rowVector");
        MultiplyThresholdMatrixJob multiplyThresholdMatrixJob =
                new MultiplyThresholdMatrixJob(n1, n2, n3, DataSetConfig.getUserItemVectorPath(),
                        threshold, averageSimilarityPath);
        runJob(multiplyThresholdMatrixJob, DataSetConfig.getUserItemVectorPath(),
                DataSetConfig.getUUThresholdPath(threshold), true);
        
        float precision = 0.001f;
        String imageFile = "img/others/similarity_distribution.png";
        String title = "similarity";
        String[] subTitles = new String[0];
        Path[] matrixDirs = new Path[] {
                DataSetConfig.getSimilarityPath(),
                DataSetConfig.getSimilarityThresholdPath(threshold),
                DataSetConfig.getSimilarityThresholdAveragePath(threshold),
                DataSetConfig.getUUThresholdPath(threshold)
        };
        String[] series = new String[] {
                "origin",
                "filter-30",
                "similarity-average-30",
                "threshold-30"
        };
        boolean withZero = true;
        boolean diagonalOnly = true;
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
        }
        iterator.close();
        coverageResult.put(name, resultCoverage);
        popularityResult.put(name, resultPopularity);
        precisionResult.put(name, resultPrecision);
        recallResult.put(name, resultRecall);
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
