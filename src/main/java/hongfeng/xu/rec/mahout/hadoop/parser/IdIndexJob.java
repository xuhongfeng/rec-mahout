/**
 * 2013-3-26
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.parser;

import hongfeng.xu.rec.mahout.hadoop.BaseJob;
import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * @author xuhongfeng
 *
 */
public class IdIndexJob extends BaseJob {

    @Override
    protected int innerRun() throws Exception {
        if (!HadoopHelper.isFileExists(getOutputPath(), getConf())) {
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