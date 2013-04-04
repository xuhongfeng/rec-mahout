/**
 * 2013-3-27
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.parser;

import hongfeng.xu.rec.mahout.config.DataSetConfig;
import hongfeng.xu.rec.mahout.hadoop.BaseJob;
import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;
import hongfeng.xu.rec.mahout.hadoop.MultipleSequenceOutputFormat;
import hongfeng.xu.rec.mahout.hadoop.misc.IntDoubleWritable;
import hongfeng.xu.rec.mahout.hadoop.misc.IntIntWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;

/**
 * @author xuhongfeng
 *
 */
public class RawDataParser extends BaseJob {
    private final Path inputAll;
    private final Path inputTraining;
    private final Path inputTest;
    
    public RawDataParser(Path inputAll, Path inputTraining, Path inputTest ) {
        super();
        this.inputAll = inputAll;
        this.inputTraining = inputTraining;
        this.inputTest = inputTest;
    }
    
    @Override
    protected void initConf(Configuration conf) {
        super.initConf(conf);
    }
    
    @Override
    protected int innerRun() throws Exception {
    
        /* create Id-Index map */
        Tool idIndexJob = new IdIndexJob();
        runJob(idIndexJob, inputAll, DataSetConfig.getIdIndexPath(), true);
        
        /* parse training data */
        if (!HadoopHelper.isFileExists(DataSetConfig.getTrainingDataPath(), getConf())) {
            Job job = prepareJob(
                    inputTraining, DataSetConfig.getTrainingDataPath(), TextInputFormat.class,
                    ParserMapper.class, IntWritable.class, IntDoubleWritable.class,
                    ParserReducer.class, IntIntWritable.class, DoubleWritable.class,
                    UserItemRate.class);
            if (!job.waitForCompletion(true)) {
                return -1;
            }
        }
        if (!HadoopHelper.isFileExists(DataSetConfig.getTestDataPath(), getConf())) {
            Job job = prepareJob(inputTest, DataSetConfig.getTestDataPath(), TextInputFormat.class,
                    ParserMapper.class, IntWritable.class, IntDoubleWritable.class,
                    ParserReducer.class, IntIntWritable.class, DoubleWritable.class,
                    UserItemRate.class);
            if (!job.waitForCompletion(true)) {
                return -1;
            }
        }
        return 0;
    }
    public static class UserItemRate extends MultipleSequenceOutputFormat<IntIntWritable, DoubleWritable> {
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