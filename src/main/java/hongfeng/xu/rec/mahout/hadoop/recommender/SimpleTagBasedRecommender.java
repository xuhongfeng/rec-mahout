/**
 * 2013-3-16
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.recommender;

import hongfeng.xu.rec.mahout.config.DeliciousDataConfig;
import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.cf.taste.hadoop.item.ItemIDIndexMapper;
import org.apache.mahout.cf.taste.hadoop.item.ItemIDIndexReducer;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VarLongWritable;

/**
 * @author xuhongfeng
 *
 */
public class SimpleTagBasedRecommender extends AbstractJob {

    @Override
    public int run(String[] args) throws Exception {
        addOutputOption();
        
        Map<String,List<String>> parsedArgs = parseArguments(args);
        if (parsedArgs == null) {
          return -1;
        }
        AtomicInteger currentPhase = new AtomicInteger();
        
        /* user tag vector */
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            if (!HadoopHelper.isFileExists(DeliciousDataConfig.getItemTagVectorPath(), getConf())) {
                Job itemIDIndex = prepareJob(DeliciousDataConfig.getUserTagPath(),
                        DeliciousDataConfig.getItemTagVectorPath(), TextInputFormat.class,
                        ItemIDIndexMapper.class, VarIntWritable.class, VarLongWritable.class, ItemIDIndexReducer.class,
                        VarIntWritable.class, VarLongWritable.class, SequenceFileOutputFormat.class);
                itemIDIndex.setCombinerClass(ItemIDIndexReducer.class);
                boolean succeeded = itemIDIndex.waitForCompletion(true);
                if (!succeeded) {
                  return -1;
                }
            }
        }
        
        /* item tag vector */
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            if (!HadoopHelper.isFileExists(DeliciousDataConfig.getItemTagVectorPath(), getConf())) {
                
            }
        }
        
        return 0;
    }

}
