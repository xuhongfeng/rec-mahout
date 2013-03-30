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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.VectorWritable;

/**
 * @author xuhongfeng
 *
 */
public abstract class BaseMatrixJob extends AbstractJob {
    private final int n1;
    private final int n2;
    private final int n3;
    private final Path multiplyerPath;
    
    public BaseMatrixJob(int n1, int n2, int n3, Path multiplyerPath) {
        super();
        this.n1 = n1;
        this.n2 = n2;
        this.n3 = n3;
        this.multiplyerPath = multiplyerPath;
    }


    protected void initConf(Configuration conf) {
        getConf().setInt("n1", n1);
        getConf().setInt("n2", n2);
        getConf().setInt("n3", n3);
        getConf().set("multiplyerPath", multiplyerPath.toString());
    }

    public int run(String[] args) throws Exception {
        addInputOption();
        addOutputOption();
        
        initConf(getConf());
        
        Map<String,List<String>> parsedArgs = parseArguments(args);
        if (parsedArgs == null) {
          return -1;
        }
        AtomicInteger currentPhase = new AtomicInteger();
        
        Path rawMatrixPath = new Path(outputDir(), "rawMatrix");
        Path rowVectorPath = new Path(outputDir(), "rowVector");
        Path columnVectorPath = new Path(outputDir(), "columnVector");
        
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            if (!HadoopHelper.isFileExists(rawMatrixPath, getConf())) {
                Job job = prepareJob(getInputPath(), rawMatrixPath, MultipleInputFormat.class,
                        MatrixMapper.class, IntWritable.class, VectorWritable.class,
                        getMatrixReducer(), IntIntWritable.class,
                        DoubleWritable.class, SequenceFileOutputFormat.class);
                job.setNumReduceTasks(10);
                if (!job.waitForCompletion(true)) {
                    return -1;
                }
            }
        }
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            if (!HadoopHelper.isFileExists(rowVectorPath, getConf())) {
                Job job = prepareJob(rawMatrixPath, rowVectorPath, MultipleInputFormat.class,
                        CombineMatrixMapper.class, IntWritable.class, IntDoubleWritable.class,
                        CombineMatrixReducer.class, IntWritable.class,
                        VectorWritable.class, SequenceFileOutputFormat.class);
                job.getConfiguration().setInt("type", CombineMatrixMapper.TYPE_ROW);
                job.getConfiguration().setInt("vectorSize", n3);
                job.getConfiguration().setInt("vectorCount", n1);
                job.setNumReduceTasks(10);
                if (!job.waitForCompletion(true)) {
                    return -1;
                }
            }
        }
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            if (!HadoopHelper.isFileExists(columnVectorPath, getConf())) {
                Job job = prepareJob(rawMatrixPath, columnVectorPath, MultipleInputFormat.class,
                        CombineMatrixMapper.class, IntWritable.class, IntDoubleWritable.class,
                        CombineMatrixReducer.class, IntWritable.class,
                        VectorWritable.class, SequenceFileOutputFormat.class);
                job.getConfiguration().setInt("type", CombineMatrixMapper.TYPE_COLUMN);
                job.getConfiguration().setInt("vectorSize", n1);
                job.getConfiguration().setInt("vectorCount", n3);
                job.setNumReduceTasks(10);
                if (!job.waitForCompletion(true)) {
                    return -1;
                }
            }
        }
        
        return 0;
    }

    protected Path outputDir() {
        return getOutputPath();
    }
    
    protected abstract Class<? extends MatrixReducer> getMatrixReducer();
}
