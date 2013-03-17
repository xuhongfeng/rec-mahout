/**
 * 2013-3-17
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.matrix;

import hongfeng.xu.rec.mahout.config.DeliciousDataConfig;
import hongfeng.xu.rec.mahout.hadoop.misc.IntDoubleWritable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
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
        
        int userCount = HadoopUtil.readInt(DeliciousDataConfig.getUserCountPath(), getConf());
        int itemCount = HadoopUtil.readInt(DeliciousDataConfig.getItemCountPath(), getConf());
        int tagCount = HadoopUtil.readInt(DeliciousDataConfig.getTagCountPath(), getConf());
        AtomicInteger currentPhase = new AtomicInteger();
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            Job job = prepareJob(DeliciousDataConfig.getUserItemPath(), DeliciousDataConfig.getUserItemMatrixPath(),
                    TextInputFormat.class, ToVectorMapper.class, KeyType.class,
                    IntDoubleWritable.class, ToVectorReducer.class, KeyType.class,
                    VectorWritable.class, UserItemVectorOutputFormat.class);
            job.getConfiguration().setInt("rowSize", userCount);
            job.getConfiguration().setInt("columnSize", itemCount);
            if (!job.waitForCompletion(true)) {
                return 1;
            }
        }
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            Job job = prepareJob(DeliciousDataConfig.getUserTagPath(), DeliciousDataConfig.getUserTagMatrixPath(),
                    TextInputFormat.class, ToVectorMapper.class, KeyType.class,
                    IntDoubleWritable.class, ToVectorReducer.class, KeyType.class,
                    VectorWritable.class, UserTagVectorOutputFormat.class);
            job.getConfiguration().setInt("rowSize", userCount);
            job.getConfiguration().setInt("columnSize", tagCount);
            if (!job.waitForCompletion(true)) {
                return 1;
            }
        }
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            Job job = prepareJob(DeliciousDataConfig.getItemTagPath(), DeliciousDataConfig.getItemTagMatrixPath(),
                    TextInputFormat.class, ToVectorMapper.class, KeyType.class,
                    IntDoubleWritable.class, ToVectorReducer.class, KeyType.class,
                    VectorWritable.class, ItemTagVectorOutputFormat.class);
            job.getConfiguration().setInt("rowSize", itemCount);
            job.getConfiguration().setInt("columnSize", tagCount);
            if (!job.waitForCompletion(true)) {
                return 1;
            }
        }
        
        return 0;
    }

    public static class UserItemVectorOutputFormat extends VectorOutputFormat {

        @Override
        protected Path getRowVectorPath() {
            return DeliciousDataConfig.getUserItemVectorPath();
        }

        @Override
        protected Path getColumnVectorPath() {
            return DeliciousDataConfig.getItemUserVectorPath();
        }
    }
    
    public static class UserTagVectorOutputFormat extends VectorOutputFormat {

        @Override
        protected Path getRowVectorPath() {
            return DeliciousDataConfig.getUserTagVectorPath();
        }

        @Override
        protected Path getColumnVectorPath() {
            return DeliciousDataConfig.getTagUserVectorPath();
        }
    }
    
    public static class ItemTagVectorOutputFormat extends VectorOutputFormat {

        @Override
        protected Path getRowVectorPath() {
            return DeliciousDataConfig.getItemTagVectorPath();
        }

        @Override
        protected Path getColumnVectorPath() {
            return DeliciousDataConfig.getTagItemVectorPath();
        }
    }
}
