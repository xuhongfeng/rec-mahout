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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.item.ItemIDIndexMapper;
import org.apache.mahout.cf.taste.hadoop.item.ItemIDIndexReducer;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VarLongWritable;

/**
 * @author xuhongfeng
 *
 */
public class EvaluateTagRecommender extends AbstractJob {
    
    public static final String OPTION_REUSE = "reuse";

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
        
        addOption(buildOption(OPTION_REUSE, "r", "reuse the old parsed data or not", true, false, ""));

        Map<String,List<String>> parsedArgs = parseArguments(args);
        if (parsedArgs == null) {
          return -1;
        }
        
        AtomicInteger currentPhase = new AtomicInteger();
        
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            RawDataParser rawDataParser = new RawDataParser();
            ToolRunner.run(rawDataParser, new String[] {
                    "-i", getInputPath().toString(),
                    "-o", DeliciousDataConfig.HDFS_OUTPUT_DIR_RAW_DATA_PARSER,
                    "--" + OPTION_REUSE, getOption(OPTION_REUSE)
            });
        }
        
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            Path inputPath = RawDataParser.getUserItemPath();
            Path outputPath = getItemIndexPath();
            if (HadoopHelper.isFileExists(outputPath, getConf())) {
                if (!hasOption(OPTION_REUSE) || !getOption(OPTION_REUSE).equals("true")) {
                    HadoopUtil.delete(getConf(), outputPath);
                    Job job= prepareJob(inputPath, outputPath, TextInputFormat.class,
                            ItemIDIndexMapper.class, VarIntWritable.class, VarLongWritable.class,
                            ItemIDIndexReducer.class, VarIntWritable.class, VarLongWritable.class,
                            SequenceFileOutputFormat.class);
                    job.setCombinerClass(ItemIDIndexReducer.class);
                    if (!job.waitForCompletion(true)) {
                        return -1;
                    }
                }
            }
        }
        
        return 0;
    }
    
    public static Path getItemIndexPath() {
        return new Path(DeliciousDataConfig.HDFS_DELICIOUS_DIR + "/itemIdIndex");
    }
}
