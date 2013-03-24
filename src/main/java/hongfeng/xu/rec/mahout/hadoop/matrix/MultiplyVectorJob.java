/**
 * 2013-3-18
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.matrix;

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
import org.apache.mahout.math.VectorWritable;

/**
 * @author xuhongfeng
 *
 */
public class MultiplyVectorJob extends AbstractJob {
    private final int n1;
    private final int n2;
    private final int n3;
    private final Path multiplyerPath;
    
    public MultiplyVectorJob(int n1, int n2, int n3, Path multiplyerPath) {
        super();
        this.n1 = n1;
        this.n2 = n2;
        this.n3 = n3;
        this.multiplyerPath = multiplyerPath;
    }



    public int run(String[] args) throws Exception {
        addInputOption();
        addOutputOption();
        
        getConf().setInt("n1", n1);
        getConf().setInt("n2", n2);
        getConf().setInt("n3", n3);
        getConf().set("multiplyerPath", multiplyerPath.toString());
        
        Map<String,List<String>> parsedArgs = parseArguments(args);
        if (parsedArgs == null) {
          return -1;
        }
        AtomicInteger currentPhase = new AtomicInteger();
        
        Path rawMatrixPath = new Path(getOutputPath(), "rawMatrix");
        Path rowVectorPath = new Path(getOutputPath(), "rowVector");
        Path columnVectorPath = new Path(getOutputPath(), "columnVector");
        
        final int n1 = getConf().getInt("n1", 0);
        final int n3 = getConf().getInt("n3", 0);
        
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            if (!HadoopHelper.isFileExists(rawMatrixPath, getConf())) {
                Job job = prepareJob(getInputPath(), rawMatrixPath, MultipleInputFormat.class,
                        MultiplyVectorMapper.class, IntWritable.class, VectorWritable.class,
                        MultiplyVectorReducer.class, IntIntWritable.class,
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
                job.getConfiguration().setInt("vectorSize", n3);
                job.getConfiguration().setInt("vectorCount", n1);
                job.setNumReduceTasks(50);
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
                job.getConfiguration().setInt("vectorSize", n1);
                job.getConfiguration().setInt("vectorCount", n3);
                job.setNumReduceTasks(50);
                if (!job.waitForCompletion(true)) {
                    return -1;
                }
            }
        }
        
        return 0;
    }

}
