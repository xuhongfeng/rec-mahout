/**
 * 2013-3-28
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.matrix;

import hongfeng.xu.rec.mahout.config.DataSetConfig;
import hongfeng.xu.rec.mahout.hadoop.BaseJob;
import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;
import hongfeng.xu.rec.mahout.hadoop.MultipleInputFormat;
import hongfeng.xu.rec.mahout.hadoop.misc.IntDoubleWritable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.math.VectorWritable;

/**
 * @author xuhongfeng
 *
 */
public class ToVectorJob extends BaseJob {

    @Override
    protected int innerRun() throws Exception {
        addInputOption();
        addOutputOption();
        
        JobQueue queue = new JobQueue();
        runJob(queue, DataSetConfig.getTrainingDataPath(),
                DataSetConfig.getUserItemVectorPath(), userCount(), itemCount(),
                ToVectorMapper.TYPE_FIRST);
        runJob(queue, DataSetConfig.getTrainingDataPath(),
                DataSetConfig.getItemUserVectorPath(), itemCount(), userCount(),
                ToVectorMapper.TYPE_SECOND);
        if (queue.waitForComplete() == -1) {
            return -1;
        }
        
        return 0;
    }
    
    private void runJob(JobQueue queue, Path inputPath, Path outputPath
            ,int vectorCount, int vectorSize, int type) throws Exception {
        if (!HadoopHelper.isFileExists(outputPath, getConf())) {
            Job job = prepareJob(inputPath, outputPath,
                    MultipleInputFormat.class, ToVectorMapper.class, IntWritable.class,
                    IntDoubleWritable.class, ToVectorReducer.class, IntWritable.class,
                    VectorWritable.class, SequenceFileOutputFormat.class);
            job.setNumReduceTasks(DataSetConfig.REDUCE_COUNT);
            job.getConfiguration().setInt("vectorCount", vectorCount);
            job.getConfiguration().setInt("vectorSize", vectorSize);
            job.getConfiguration().setInt("type", type);
            queue.submitJob(job);
        }
    }
}