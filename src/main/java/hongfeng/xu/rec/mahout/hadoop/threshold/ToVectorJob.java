/**
 * 2013-3-28
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.threshold;

import hongfeng.xu.rec.mahout.config.MovielensDataConfig;
import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;
import hongfeng.xu.rec.mahout.hadoop.MultipleInputFormat;
import hongfeng.xu.rec.mahout.hadoop.misc.IntDoubleWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * @author xuhongfeng
 *
 */
public class ToVectorJob extends AbstractJob {

    @Override
    public int run(String[] args) throws Exception {
        
        addInputOption();
        addOutputOption();
        
        Map<String,List<String>> parsedArgs = parseArguments(args);
        if (parsedArgs == null) {
          return -1;
        }
        
        int userCount = HadoopUtil.readInt(MovielensDataConfig.getUserCountPath(), getConf());
        int itemCount = HadoopUtil.readInt(MovielensDataConfig.getItemCountPath(), getConf());
        
        AtomicInteger currentPhase = new AtomicInteger();
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            List<Job> jobs = new ArrayList<Job>();
            runJob(jobs, MovielensDataConfig.getRawTrainingDataPath(),
                    MovielensDataConfig.getUserItemVectorPath(), userCount, itemCount,
                    ToVectorMapper.TYPE_FIRST);
            runJob(jobs, MovielensDataConfig.getRawTrainingDataPath(),
                    MovielensDataConfig.getItemUserVectorPath(), itemCount, userCount,
                    ToVectorMapper.TYPE_SECOND);
            while (jobs.size() > 0) {
                Iterator<Job> iterator = jobs.iterator();
                while (iterator.hasNext()) {
                    Job job = iterator.next();
                    if (job.isComplete()) {
                        if (!job.isSuccessful()) {
                            return -1;
                        }
                        iterator.remove();
                    }
                }
                Thread.sleep(1000L);
            }
        }
        
        if (!HadoopHelper.isFileExists(MovielensDataConfig.getUserItemOneZeroVectorPath(), getConf())) {
            Job job = prepareJob(MovielensDataConfig.getUserItemVectorPath(),
                    MovielensDataConfig.getUserItemOneZeroVectorPath(), MultipleInputFormat.class,
                    ToOneZeroMapper.class, IntWritable.class, VectorWritable.class,
                    SequenceFileOutputFormat.class);
            if (!job.waitForCompletion(true)) {
                return -1;
            }
        }
        if (!HadoopHelper.isFileExists(MovielensDataConfig.getItemUserOneZeroVectorPath(), getConf())) {
            Job job = prepareJob(MovielensDataConfig.getItemUserVectorPath(),
                    MovielensDataConfig.getItemUserOneZeroVectorPath(), MultipleInputFormat.class,
                    ToOneZeroMapper.class, IntWritable.class, VectorWritable.class,
                    SequenceFileOutputFormat.class);
            if (!job.waitForCompletion(true)) {
                return -1;
            }
        }
        return 0;
    }
    
    private void runJob(List<Job> list, Path inputPath, Path outputPath
            ,int vectorCount, int vectorSize, int type) throws Exception {
        Job job = prepareJob(inputPath, outputPath,
                MultipleInputFormat.class, ToVectorMapper.class, IntWritable.class,
                IntDoubleWritable.class, ToVectorReducer.class, IntWritable.class,
                VectorWritable.class, SequenceFileOutputFormat.class);
        job.setNumReduceTasks(10);
        job.getConfiguration().setInt("vectorSize", vectorSize);
        job.getConfiguration().setInt("type", type);
        job.submit();
        list.add(job);
    }
    
    public static class ToOneZeroMapper extends Mapper<IntWritable, VectorWritable,
        IntWritable, VectorWritable> {

        public ToOneZeroMapper() {
            super();
        }
        
        @Override
        protected void map(IntWritable key, VectorWritable value, Context context)
                throws IOException, InterruptedException {
            Vector vector = value.get();
            Vector newVector = new RandomAccessSparseVector(vector.size(),
                    vector.getNumNondefaultElements());
            Iterator<Vector.Element> iterator = vector.iterateNonZero();
            while (iterator.hasNext()) {
                Vector.Element e = iterator.next();
                newVector.setQuick(e.index(), 1);
            }
            value.set(newVector);
            context.write(key, value);
        }
    }
}