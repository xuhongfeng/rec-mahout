/**
 * 2013-3-12
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.eval;

import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;
import hongfeng.xu.rec.mahout.hadoop.recommender.BaseRecommender;
import hongfeng.xu.rec.mahout.structure.RecommendedItemsAndUserIdWritable;
import hongfeng.xu.rec.mahout.structure.TypeAndNWritable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;

/**
 * @author xuhongfeng
 *
 */
public class EvaluateRecommenderJob<T extends BaseRecommender> extends AbstractJob {
    
    private final T recommenderJob;
    private final Path recommendResultPath;

    public EvaluateRecommenderJob(T recommenderJob, Path recommendResultPath) {
        super();
        this.recommenderJob = recommenderJob;
        this.recommendResultPath = recommendResultPath;
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
        
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            runJob(recommenderJob, new String[]{}, getInputPath(), recommendResultPath);
        }
        
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            if (!HadoopHelper.isFileExists(getOutputPath(), getConf())) {
                Job job = prepareJob(recommendResultPath, getOutputPath(), SequenceFileInputFormat.class,
                        TopNMapper.class, TypeAndNWritable.class, RecommendedItemsAndUserIdWritable.class,
                        TopNReducer.class, TypeAndNWritable.class, DoubleWritable.class,
                        SequenceFileOutputFormat.class);
                if (!job.waitForCompletion(true)) {
                    return -1;
                }
            }
        }
        
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
