/**
 * 2013-3-10
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop;

import hongfeng.xu.rec.mahout.chart.ChartDrawer;
import hongfeng.xu.rec.mahout.config.DeliciousDataConfig;
import hongfeng.xu.rec.mahout.hadoop.eval.EvaluateRecommenderJob;
import hongfeng.xu.rec.mahout.hadoop.eval.TypeAndNWritable;
import hongfeng.xu.rec.mahout.hadoop.parser.RawDataParser;
import hongfeng.xu.rec.mahout.hadoop.recommender.RandomRecommender;
import hongfeng.xu.rec.mahout.runner.AbsTopNRunner.Result;
import hongfeng.xu.rec.mahout.util.L;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.preparation.PreparePreferenceMatrixJob;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterator;

/**
 * @author xuhongfeng
 *
 */
public class EvaluateTagRecommender extends AbstractJob {
    
    public static void main(String[] args) {
        EvaluateTagRecommender job = new EvaluateTagRecommender();
        try {
            ToolRunner.run(job, args);
        } catch (Exception e) {
            L.e(job, e);
        }
    }
    
    @Override
    public int run(String[] args) throws Exception {
        
        addInputOption();
        addOutputOption();
        
        Map<String,List<String>> parsedArgs = parseArguments(args);
        if (parsedArgs == null) {
          return -1;
        }
        
        AtomicInteger currentPhase = new AtomicInteger();
        
        /** parse raw data **/
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            RawDataParser rawDataParser = new RawDataParser();
            runJob(rawDataParser, null, getInputPath(), DeliciousDataConfig.getRawDataPath());
        }
        
        /** prepare martix **/
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            runJob(new PreparePreferenceMatrixJob(), new String[] {
                    "--booleanData", String.valueOf(false),
                    "--tempDir", getTempPath().toString()
            }, DeliciousDataConfig.getUserItemPath(), DeliciousDataConfig.getUserItemMatrixPath());
        }
        
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            runJob(new EvaluateRecommenderJob<RandomRecommender>(new RandomRecommender(),
                    DeliciousDataConfig.getRandomRecommenderResultPath()), new String[] {},
                DeliciousDataConfig.getUserItemVectors(),
                DeliciousDataConfig.getEvaluatePath());
        }
        
        SequenceFileDirIterator<TypeAndNWritable, DoubleWritable> iterator =
                new SequenceFileDirIterator<TypeAndNWritable, DoubleWritable>(
                        DeliciousDataConfig.getEvaluatePath(), PathType.LIST,
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
        Map<String, Result> resultMap = new HashMap<String, Result>();
        resultMap.put("random", resultCoverage);
        ChartDrawer chartDrawer = new ChartDrawer("Coverage Rate", "coverage", "img/coverage.png", resultMap, true);
        chartDrawer.draw();
        resultMap.clear();
        resultMap.put("random", resultPrecision);
        chartDrawer = new ChartDrawer("Precision Rate", "precision", "img/precision.png", resultMap, true);
        chartDrawer.draw();
        resultMap.clear();
        resultMap.put("random", resultRecall);
        chartDrawer = new ChartDrawer("Recall Rate", "recall", "img/recall.png", resultMap, true);
        chartDrawer.draw();
        resultMap.clear();
        resultMap.put("random", resultPopularity);
        chartDrawer = new ChartDrawer("Popularity", "popularity", "img/popularity.png", resultMap, false);
        chartDrawer.draw();
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
    
}
