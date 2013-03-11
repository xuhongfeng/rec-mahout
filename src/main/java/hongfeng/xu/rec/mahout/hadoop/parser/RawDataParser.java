/**
 * 2013-3-10
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.parser;

import hongfeng.xu.rec.mahout.config.DeliciousDataConfig;
import hongfeng.xu.rec.mahout.hadoop.DataUtils;
import hongfeng.xu.rec.mahout.util.L;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;

/**
 * @author xuhongfeng
 *
 */
public class RawDataParser extends AbstractJob {
    
    @Override
    public int run(String[] args) throws Exception {
        addInputOption();
        addOutputOption();
        
        
        Map<String,List<String>> parsedArgs = parseArguments(args);
        if (parsedArgs == null) {
          return -1;
        }
        AtomicInteger currentPhase = new AtomicInteger();
        
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            Job job = prepareJob(getInputPath(), getOutputPath(), TextInputFormat.class, 
                    ParserMapper.class, KeyType.class, DoubleWritable.class, ParserReducer.class,
                    KeyType.class, DoubleWritable.class, ParserOutputFormat.class);
            if (!job.waitForCompletion(true)) {
                return -1;
            }
        }
        
        FastIDSet itemIdSet = DataUtils.parseItemIdSetFromHDFS(getConf());
        HadoopUtil.writeInt(itemIdSet.size(), DeliciousDataConfig.getItemCountPath(),
                getConf());
        
        return 0;
    }
    
    
    public static void main(String args[]) {
        RawDataParser job = new RawDataParser();
        try {
            ToolRunner.run(job, args);
        } catch (Exception e) {
            L.e(job, e);
        }
    }
    
} 