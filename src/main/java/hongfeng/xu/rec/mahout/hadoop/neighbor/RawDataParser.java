/**
 * 2013-3-27
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.neighbor;

import hongfeng.xu.rec.mahout.config.MovielensDataConfig;
import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;
import hongfeng.xu.rec.mahout.hadoop.MultipleSequenceOutputFormat;
import hongfeng.xu.rec.mahout.hadoop.misc.IntIntWritable;
import hongfeng.xu.rec.mahout.util.L;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;

/**
 * @author xuhongfeng
 *
 */
public class RawDataParser extends AbstractJob {
    
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
            if (!HadoopHelper.isFileExists(MovielensDataConfig.getIdIndexPath(), getConf())) {
                Tool job = new IdIndexJob();
                ToolRunner.run(job, new String[] {
                    "--input", MovielensDataConfig.getAllDataPath().toString(),
                    "--output", MovielensDataConfig.getIdIndexPath().toString(),
                });
            }
        }
        
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            if (!HadoopHelper.isFileExists(MovielensDataConfig.getRawTrainingDataPath(), getConf())) {
                Job job = prepareJob(MovielensDataConfig.getTrainingDataPath(),
                        MovielensDataConfig.getRawTrainingDataPath(), TextInputFormat.class, 
                        ParserMapper.class, IntWritable.class, IntIntWritable.class,
                        ParserReducer.class,
                        IntIntWritable.class, IntWritable.class, UserItemRate.class);
                if (!job.waitForCompletion(true)) {
                    return -1;
                }
            }
        }
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            if (!HadoopHelper.isFileExists(MovielensDataConfig.getRawTestDataPath(), getConf())) {
                Job job = prepareJob(MovielensDataConfig.getTestDataPath(),
                        MovielensDataConfig.getRawTestDataPath(), TextInputFormat.class, 
                        ParserMapper.class, IntWritable.class, IntIntWritable.class,
                        ParserReducer.class, IntIntWritable.class, IntWritable.class,
                        UserItemRate.class);
                if (!job.waitForCompletion(true)) {
                    return -1;
                }
            }
        }
        
        return 0;
    }
    
    
    public static void main(String args[]) {
        RawDataParser job = new RawDataParser();
        try {
            ToolRunner.run(job, args);
        } catch (Exception e) {
            L.e(job, e);
        }
    }
    
    public static class UserItemRate extends MultipleSequenceOutputFormat<IntIntWritable, IntWritable> {
        @Override
        protected String getFile(IntIntWritable key, Configuration conf) {
            int totalCount = conf.getInt("totalCount", -1);
            if (totalCount == -1) {
                return String.valueOf(key.getId1()%10);
            } else {
                return String.valueOf(key.getId1()/(totalCount/10));
            }
        }
    }
} 