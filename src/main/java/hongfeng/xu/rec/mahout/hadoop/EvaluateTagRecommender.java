/**
 * 2013-3-10
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop;

import hongfeng.xu.rec.mahout.config.DeliciousDataConfig;
import hongfeng.xu.rec.mahout.hadoop.parser.RawDataParser;
import hongfeng.xu.rec.mahout.util.L;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;

/**
 * @author xuhongfeng
 *
 */
public class EvaluateTagRecommender extends AbstractJob {

    public static void main(String[] args) {
        EvaluateTagRecommender job = new EvaluateTagRecommender();
        try {
            ToolRunner.run(job, args);
        } catch (Exception e) {
            L.e(job, e);
        }
    }
    
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
            RawDataParser rawDataParser = new RawDataParser();
            ToolRunner.run(rawDataParser, new String[] {
                    "-i", getInputPath().toString(),
                    "-o", DeliciousDataConfig.HDFS_OUTPUT_DIR_RAW_DATA_PARSER
            });
        }
        
        return 0;
    }
}
