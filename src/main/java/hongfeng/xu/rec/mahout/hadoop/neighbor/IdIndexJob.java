/**
 * 2013-3-26
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.neighbor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.mahout.common.AbstractJob;

/**
 * @author xuhongfeng
 *
 */
public class IdIndexJob extends AbstractJob {

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
            Job job = prepareJob(getInputPath(), getOutputPath(), IdIndexMapper.class,
                    IntWritable.class, LongWritable.class,
                    IdIndexReducer.class, NullWritable.class, NullWritable.class);
            job.setInputFormatClass(TextInputFormat.class);
            if (!job.waitForCompletion(true)) {
                return -1;
            }
        }
        
        return 0;
    }
}