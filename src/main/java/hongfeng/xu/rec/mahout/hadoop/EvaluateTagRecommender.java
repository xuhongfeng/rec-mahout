/**
 * 2013-3-10
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop;

import hongfeng.xu.rec.mahout.chart.ChartDrawer;
import hongfeng.xu.rec.mahout.config.DeliciousDataConfig;
import hongfeng.xu.rec.mahout.hadoop.eval.EvaluateCoverageJob;
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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.preparation.PreparePreferenceMatrixJob;
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
public class EvaluateTagRecommender extends AbstractJob {
    
    public static final String OPTION_REUSE = "reuse";

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
        
        addOption(buildOption(OPTION_REUSE, "r", "reuse the old parsed data or not", true, false, ""));

        Map<String,List<String>> parsedArgs = parseArguments(args);
        if (parsedArgs == null) {
          return -1;
        }
        
        AtomicInteger currentPhase = new AtomicInteger();
        
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            RawDataParser rawDataParser = new RawDataParser();
            runJob(rawDataParser, null, getInputPath(), DeliciousDataConfig.getRawDataPath());
        }
        
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            runJob(new PreparePreferenceMatrixJob(), new String[] {
                    "--booleanData", String.valueOf(false),
                    "--tempDir", getTempPath().toString()
            }, DeliciousDataConfig.getUserItemPath(), DeliciousDataConfig.getUserItemMatrixPath());
        }
        
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            runJob(new RandomRecommender(), new String[] {},
                DeliciousDataConfig.getUserItemVectors(),
                DeliciousDataConfig.getRandomRecommenderResultPath());
        }
        
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            runJob(new EvaluateCoverageJob(), new String[] {},
                DeliciousDataConfig.getRandomRecommenderResultPath(),
                DeliciousDataConfig.getCoverageResultPath());
        }
        
        SequenceFileDirIterator<IntWritable, DoubleWritable> iterator =
                new SequenceFileDirIterator<IntWritable, DoubleWritable>(
                        DeliciousDataConfig.getCoverageResultPath(), PathType.LIST,
                        PathFilters.partFilter(), null, false, getConf());
        Result result = new Result();
        while (iterator.hasNext()) {
            Pair<IntWritable, DoubleWritable> pair = iterator.next();
            result.put(pair.getFirst().get(), pair.getSecond().get());
        }
        iterator.close();
        Map<String, Result> resultMap = new HashMap<String, Result>();
        resultMap.put("random", result);
        ChartDrawer chartDrawer = new ChartDrawer("Coverage Rate", "coverage", "coverage.png", resultMap, true);
        chartDrawer.draw();
        return 0;
    }
    
    private void runJob (Tool job, String[] args, Path input, Path output) throws Exception {
        boolean needRun = true;
        if (HadoopHelper.isFileExists(output, getConf())) {
            if (reuse()) {
                needRun = false;
            } else {
                HadoopUtil.delete(getConf(), output);
            }
        }
        if (needRun) {
            args = (String[]) ArrayUtils.addAll(new String[] {
                "--input", input.toString(),
                "--output", output.toString(),
            }, args);
            ToolRunner.run(getConf(), job, args);
        }
    }
    
    private boolean reuse() {
        return hasOption(OPTION_REUSE) && getOption(OPTION_REUSE).equals("true");
    }
}
