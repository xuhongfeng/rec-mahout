/**
 * 2013-3-17
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop;

import hongfeng.xu.rec.mahout.chart.ChartDrawer;
import hongfeng.xu.rec.mahout.config.DeliciousDataConfig;
import hongfeng.xu.rec.mahout.hadoop.eval.EvaluateRecommenderJob;
import hongfeng.xu.rec.mahout.hadoop.eval.TypeAndNWritable;
import hongfeng.xu.rec.mahout.hadoop.matrix.ToVectorJob;
import hongfeng.xu.rec.mahout.hadoop.parser.RawDataParser;
import hongfeng.xu.rec.mahout.hadoop.recommender.PopularRecommender;
import hongfeng.xu.rec.mahout.hadoop.recommender.RandomRecommender;
import hongfeng.xu.rec.mahout.hadoop.recommender.SimpleTagBasedRecommender;
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
                    DeliciousDataConfig.getRawDataPath());
        }
        
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            ToVectorJob job = new ToVectorJob();
            runJob(job, new String[] {}, DeliciousDataConfig.getRawDataPath(),
                    DeliciousDataConfig.getMatrixPath());
        }
        
        /* random recommender */
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            runJob(new EvaluateRecommenderJob<RandomRecommender>(new RandomRecommender(),
                    DeliciousDataConfig.getRandomRecommenderResultPath()), new String[] {},
                DeliciousDataConfig.getUserItemVectorPath(),
                DeliciousDataConfig.getRandomRecommenderEvaluate());
        }
        
        /* popular recommender */
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            runJob(new EvaluateRecommenderJob<PopularRecommender>(new PopularRecommender(),
                    DeliciousDataConfig.getPopularRecommenderResultPath()), new String[] {},
                DeliciousDataConfig.getUserItemVectorPath(),
                DeliciousDataConfig.getPopularRecommederEvaluate());
        }
        
        /* simple tag based recommender */
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            runJob(new EvaluateRecommenderJob<SimpleTagBasedRecommender>(new SimpleTagBasedRecommender(),
                    DeliciousDataConfig.getSimpleTagBasedResult()), new String[] {},
                DeliciousDataConfig.getUserItemVectorPath(),
                DeliciousDataConfig.getSimpleTagBasedEvaluate());
        }
        
        
        calculateResult(DeliciousDataConfig.getRandomRecommenderEvaluate(), "random");
        calculateResult(DeliciousDataConfig.getPopularRecommederEvaluate(), "popular");
        calculateResult(DeliciousDataConfig.getSimpleTagBasedEvaluate(), "simpleTagBased");
        
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
    
    private void runJob (Tool job, String[] args, Path input, Path output) throws Exception {
        if (!HadoopHelper.isFileExists(output, getConf())) {
            args = (String[]) ArrayUtils.addAll(new String[] {
                "--input", input.toString(),
                "--output", output.toString(),
            }, args);
            ToolRunner.run(getConf(), job, args);
        }
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
