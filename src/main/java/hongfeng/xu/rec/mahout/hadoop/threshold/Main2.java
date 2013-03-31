/**
 * 2013-3-31
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.threshold;

import hongfeng.xu.rec.mahout.chart.ChartDrawer;
import hongfeng.xu.rec.mahout.config.MovielensDataConfig;
import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;
import hongfeng.xu.rec.mahout.hadoop.matrix.MultiplyMatrixJob;
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
public class Main2 extends AbstractJob {
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
        
        int userCount = HadoopUtil.readInt(MovielensDataConfig.getUserCountPath(), getConf());
        int itemCount = HadoopUtil.readInt(MovielensDataConfig.getItemCountPath(), getConf());
        
        //draw one-zero distribution
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            int n1 = itemCount;
            int n2 = userCount;
            int n3 = n1;
            Path multipyerPath = MovielensDataConfig.getItemUserOneZeroVectorPath();
            MultiplyMatrixJob job = new MultiplyMatrixJob(n1, n2, n3, multipyerPath);
            runJob(job, new String[] {}, MovielensDataConfig.getItemUserOneZeroVectorPath(),
                    MovielensDataConfig.getIUUIOneZero());
        }
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            CountOneZeroJob job = new CountOneZeroJob();
            Path input = MovielensDataConfig.getIUUIOneZero();
            Path output = MovielensDataConfig.getCountIUUIOneZeroPath();
            runJob(job, new String[] {}, input, output);
        }
        
        new DrawCountOneZero(MovielensDataConfig.getCountIUUIOneZeroPath(),
                "item-item count", "img/others/item-item-count.png").draw(getConf());
        
        
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
        /* item based recommender */
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            runJob(new EvaluateRecommenderJob<ItemBasedRecommender>(new ItemBasedRecommender(),
                    MovielensDataConfig.getItemBasedResult()), new String[] {},
                MovielensDataConfig.getUserItemVectorPath(),
                MovielensDataConfig.getItemBasedEvaluate());
        }
        
        calculateResult(MovielensDataConfig.getRandomRecommenderEvaluate(), "random");
        calculateResult(MovielensDataConfig.getPopularRecommederEvaluate(), "popular");
        calculateResult(MovielensDataConfig.getItemBasedEvaluate(), "ItemBased");
        
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
        Main2 job = new Main2();
        try {
            ToolRunner.run(job, args);
        } catch (Exception e) {
            L.e(job, e);
        }
    }

}
