/**
 * 2013-3-18
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.matrix;

import hongfeng.xu.rec.mahout.hadoop.misc.IntDoubleWritable;
import hongfeng.xu.rec.mahout.hadoop.misc.IntIntWritable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.VectorWritable;

/**
 * @author xuhongfeng
 *
 */
public class MultiplyVectorJob extends AbstractJob {
    
    public static final String OPTION_RAW_MATRIX_PATH = "rawMatrixPath";
    
    private final Class<? extends MultiplyVectorMapper> mapperClass;

    public MultiplyVectorJob(Class<? extends MultiplyVectorMapper> mapperClass) {
        super();
        this.mapperClass = mapperClass;
    }



    public int run(String[] args) throws Exception {
        addInputOption();
        addOutputOption();
        addOption(buildOption(OPTION_RAW_MATRIX_PATH, "r", "path of IntIntWritable DoubleWritable", true, true, ""));
        
        Map<String,List<String>> parsedArgs = parseArguments(args);
        if (parsedArgs == null) {
          return -1;
        }
        AtomicInteger currentPhase = new AtomicInteger();
        
        String pathStr = getOption(OPTION_RAW_MATRIX_PATH);
        Path rawMatrixPath = new Path(pathStr);
        
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            Job job = prepareJob(inputPath, rawMatrixPath, SequenceFileInputFormat.class,
                    mapperClass, IntIntWritable.class, VectorPair.class,
                    MultiplyVectorReducer.class, IntIntWritable.class,
                    DoubleWritable.class, SequenceFileOutputFormat.class);
            if (!job.waitForCompletion(true)) {
                return -1;
            }
        }
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            Job job = prepareJob(rawMatrixPath, getOutputPath(), SequenceFileInputFormat.class,
                    CombineMultiplyMapper.class, IntWritable.class, IntDoubleWritable.class,
                    CombineMultiplyReducer.class, IntWritable.class,
                    VectorWritable.class, SequenceFileOutputFormat.class);
            if (!job.waitForCompletion(true)) {
                return -1;
            }
        }
        
        return 0;
    }

}
