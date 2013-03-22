/**
 * 2013-3-18
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.matrix;

import hongfeng.xu.rec.mahout.config.DeliciousDataConfig;
import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;
import hongfeng.xu.rec.mahout.hadoop.MultipleInputFormat;
import hongfeng.xu.rec.mahout.hadoop.misc.IntDoubleWritable;
import hongfeng.xu.rec.mahout.hadoop.misc.IntIntWritable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.VectorWritable;

/**
 * @author xuhongfeng
 *
 */
public class MultiplyVectorJob extends AbstractJob {
    
    private final Class<? extends MultiplyVectorReducer> reducerClass;

    public MultiplyVectorJob(Class<? extends MultiplyVectorReducer> reducerClass) {
        super();
        this.reducerClass = reducerClass;
    }



    public int run(String[] args) throws Exception {
        addInputOption();
        addOutputOption();
        
        Map<String,List<String>> parsedArgs = parseArguments(args);
        if (parsedArgs == null) {
          return -1;
        }
        AtomicInteger currentPhase = new AtomicInteger();
        
        Path rawMatrixPath = new Path(getOutputPath(), "rawMatrix");
        Path rowVectorPath = new Path(getOutputPath(), "rowVector");
        Path columnVectorPath = new Path(getOutputPath(), "columnVector");
        
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            if (!HadoopHelper.isFileExists(rawMatrixPath, getConf())) {
                Job job = prepareJob(getInputPath(), rawMatrixPath, MultipleInputFormat.class,
                        MultiplyVectorMapper.class, IntWritable.class, VectorWritable.class,
                        reducerClass, IntIntWritable.class,
                        DoubleWritable.class, RawMatrixOutputFormat.class);
                job.setNumReduceTasks(10);
                if (!job.waitForCompletion(true)) {
                    return -1;
                }
            }
        }
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            if (!HadoopHelper.isFileExists(rowVectorPath, getConf())) {
                Job job = prepareJob(rawMatrixPath, rowVectorPath, MultipleInputFormat.class,
                        CombineMultiplyMapper.class, IntWritable.class, IntDoubleWritable.class,
                        CombineMultiplyReducer.class, IntWritable.class,
                        VectorWritable.class, VectorOutputFormat.class);
                job.getConfiguration().setInt("type", CombineMultiplyMapper.TYPE_ROW);
                job.getConfiguration().setInt("vectorSize", HadoopUtil.readInt(DeliciousDataConfig.getItemCountPath(), getConf()));
                job.setNumReduceTasks(10);
                if (!job.waitForCompletion(true)) {
                    return -1;
                }
            }
        }
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            if (!HadoopHelper.isFileExists(columnVectorPath, getConf())) {
                Job job = prepareJob(rawMatrixPath, columnVectorPath, MultipleInputFormat.class,
                        CombineMultiplyMapper.class, IntWritable.class, IntDoubleWritable.class,
                        CombineMultiplyReducer.class, IntWritable.class,
                        VectorWritable.class, VectorOutputFormat.class);
                job.getConfiguration().setInt("type", CombineMultiplyMapper.TYPE_COLUMN);
                job.getConfiguration().setInt("vectorSize", HadoopUtil.readInt(DeliciousDataConfig.getUserCountPath(), getConf()));
                job.setNumReduceTasks(10);
                if (!job.waitForCompletion(true)) {
                    return -1;
                }
            }
        }
        
        return 0;
    }

}
