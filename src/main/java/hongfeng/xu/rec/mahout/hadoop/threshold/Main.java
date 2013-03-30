/**
 * 2013-3-26
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.threshold;

import hongfeng.xu.rec.mahout.chart.ChartDrawer;
import hongfeng.xu.rec.mahout.config.MovielensDataConfig;
import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;
import hongfeng.xu.rec.mahout.hadoop.recommender.PopularRecommender;
import hongfeng.xu.rec.mahout.hadoop.recommender.RandomRecommender;
import hongfeng.xu.rec.mahout.runner.AbsTopNRunner.Result;
import hongfeng.xu.rec.mahout.util.L;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterator;

/**
 * @author xuhongfeng
 *
 */
public class Main extends AbstractJob {
    private Map<String, Result> coverageResult = new HashMap<String, Result>();
    private Map<String, Result> popularityResult = new HashMap<String, Result>();
    private Map<String, Result> precisionResult = new HashMap<String, Result>();
    private Map<String, Result> recallResult = new HashMap<String, Result>();
    
    @Override
    public int run(String[] args) throws Exception {
        
        addInputOption();
        addOutputOption();
        
        Map<String,List<String>> parsedArgs = parseArguments(args);
        if (parsedArgs == null) {
          return -1;
        }
        
        AtomicInteger currentPhase = new AtomicInteger();
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            RawDataParser job = new RawDataParser();
            runJob(job, new String[] {}, getInputPath(),
                MovielensDataConfig.getRawDataPath());
        }
        
        int userCount = HadoopUtil.readInt(MovielensDataConfig.getUserCountPath(), getConf());
        int itemCount = HadoopUtil.readInt(MovielensDataConfig.getItemCountPath(), getConf());
        
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            ToVectorJob job = new ToVectorJob();
            runJob(job, new String[] {}, MovielensDataConfig.getRawDataPath(),
                    MovielensDataConfig.getMatrixPath());
        }
        
        //draw one-zero distribution
//        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
//            int n1 = HadoopUtil.readInt(MovielensDataConfig.getUserCountPath(), getConf());
//            int n2 = HadoopUtil.readInt(MovielensDataConfig.getItemCountPath(), getConf());
//            int n3 = n1;
//            Path multipyerPath = MovielensDataConfig.getUserItemOneZeroVectorPath();
//            MultiplyMatrixJob job = new MultiplyMatrixJob(n1, n2, n3, multipyerPath);
//            runJob(job, new String[] {}, MovielensDataConfig.getUserItemOneZeroVectorPath(),
//                    MovielensDataConfig.getUIIUOneZero());
//        }
//        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
//            CountUIIUOneZeroJob job = new CountUIIUOneZeroJob();
//            Path input = new Path(MovielensDataConfig.getUIIUOneZero(), "rowVector");
//            Path output = MovielensDataConfig.getCountUIIUOneZeroPath();
//            runJob(job, new String[] {}, input, output);
//        }
//        
//        new DrawCountUUOneZero().draw(getConf());
        
        
        /* random recommender */
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            runJob(new EvaluateRecommenderJob<RandomRecommender>(new RandomRecommender(),
                    MovielensDataConfig.getRandomRecommenderResultPath()), new String[] {},
                MovielensDataConfig.getUserItemVectorPath(),
                MovielensDataConfig.getRandomRecommenderEvaluate());
        }
        
        /* popular recommender */
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            runJob(new EvaluateRecommenderJob<PopularRecommender>(new PopularRecommender(),
                    MovielensDataConfig.getPopularRecommenderResultPath()), new String[] {},
                MovielensDataConfig.getUserItemVectorPath(),
                MovielensDataConfig.getPopularRecommederEvaluate());
        }
        /* user based recommender */
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            runJob(new EvaluateRecommenderJob<UserBasedRecommender>(new UserBasedRecommender(),
                    MovielensDataConfig.getUserBasedResult()), new String[] {},
                MovielensDataConfig.getUserItemVectorPath(),
                MovielensDataConfig.getUserBasedEvaluate());
            
        }
        
        /* threshold recommender */
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            runJob(new EvaluateRecommenderJob<ThresholdRecommender>(new ThresholdRecommender(),
                    MovielensDataConfig.getThresholdResult()), new String[] {},
                MovielensDataConfig.getUserItemVectorPath(),
                MovielensDataConfig.getThresholdEvaluate());
        }
        
        calculateResult(MovielensDataConfig.getRandomRecommenderEvaluate(), "random");
        calculateResult(MovielensDataConfig.getPopularRecommederEvaluate(), "popular");
        calculateResult(MovielensDataConfig.getUserBasedEvaluate(), "UserBased");
        calculateResult(MovielensDataConfig.getThresholdEvaluate(), "Threshold");
        
        ChartDrawer chartDrawer = new ChartDrawer("Coverage Rate", "coverage", "img/coverage.png", coverageResult, true);
        chartDrawer.draw();
        chartDrawer = new ChartDrawer("Precision Rate", "precision", "img/precision.png", precisionResult, true);
        chartDrawer.draw();
        chartDrawer = new ChartDrawer("Recall Rate", "recall", "img/recall.png", recallResult, true);
        chartDrawer.draw();
        chartDrawer = new ChartDrawer("Popularity", "popularity", "img/popularity.png", popularityResult, false);
        chartDrawer.draw();
        
        /* draw user similarity distribution */
//        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
//            if (!HadoopHelper.isFileExists(MovielensDataConfig.getUserCosineSimilarityPath(), getConf())) {
//                CosineSimilarityJob job = new CosineSimilarityJob(userCount,
//                        itemCount, userCount, MovielensDataConfig.getUserItemVectorPath());
//                ToolRunner.run(job, new String[] {
//                        "--input", MovielensDataConfig.getUserItemVectorPath().toString(),
//                        "--output", MovielensDataConfig.getUserCosineSimilarityPath().toString()
//                });
//            }
//        }
//        
//        
//        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
//            int mode = DrawMatrixJob.MODE_WITH_ZERO;
//            float precesion = 0.001f;
//            String imageFile = "img/distribution_UU_cosine_sim.png";
//            String title = "user similarity distribution";
//            DrawMatrixJob job = new DrawMatrixJob(mode, precesion, imageFile, title
//                    , new String[] {
//                    String.format("userCount = %d", userCount)
//            });
//            Path input = MovielensDataConfig.getUserCosineSimilarityPath();
//            Path output = input;
//            ToolRunner.run(job, new String[] {
//                    "--input", input.toString(),
//                "--output", output.toString(),
//            });
//        }
        
        return 0;
    }
    
    private void runJob (Tool job, String[] args, Path input, Path output) throws Exception {
        if (!HadoopHelper.isFileExists(output, getConf())) {
            args = (String[]) ArrayUtils.addAll(new String[] {
                "--input", input.toString(),
                "--output", output.toString(),
            }, args);
            ToolRunner.run(getConf(), job, args);
        }
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
