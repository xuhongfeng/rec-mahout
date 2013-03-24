/**
 * 2013-3-23
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.recommender;

import hongfeng.xu.rec.mahout.config.DeliciousDataConfig;
import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;
import hongfeng.xu.rec.mahout.hadoop.matrix.MultiplyVectorJob;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.HadoopUtil;

/**
 * @author xuhongfeng
 *
 */
public class XieFengRecommender extends BaseRecommender {

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
            if (!HadoopHelper.isFileExists(DeliciousDataConfig.getUIIUPath(), getConf())) {
                int n1 = HadoopUtil.readInt(DeliciousDataConfig.getUserCountPath(), getConf());
                int n2 = HadoopUtil.readInt(DeliciousDataConfig.getItemCountPath(), getConf());
                int n3 = HadoopUtil.readInt(DeliciousDataConfig.getUserCountPath(), getConf());
                Path multipyerPath = DeliciousDataConfig.getUserItemVectorPath();
                MultiplyVectorJob job = new MultiplyVectorJob(n1, n2, n3, multipyerPath);
                ToolRunner.run(job, new String[] {
                        "--input", DeliciousDataConfig.getUserItemVectorPath().toString(),
                        "--output", DeliciousDataConfig.getUIIUPath().toString()
                });
            }
        }
        
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            if (!HadoopHelper.isFileExists(getUUUIPath(), getConf())) {
                int n1 = HadoopUtil.readInt(DeliciousDataConfig.getUserCountPath(), getConf());
                int n2 = HadoopUtil.readInt(DeliciousDataConfig.getUserCountPath(), getConf());
                int n3 = HadoopUtil.readInt(DeliciousDataConfig.getItemCountPath(), getConf());
                Path multipyerPath = DeliciousDataConfig.getItemUserVectorPath();
                MultiplyVectorJob job = new MultiplyVectorJob(n1, n2, n3, multipyerPath);
                ToolRunner.run(job, new String[] {
                        "--input", DeliciousDataConfig.getUIIURowVectorPath().toString(),
                        "--output", getUUUIPath().toString()
                });
            }
        }
        
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            if (!HadoopHelper.isFileExists(getOutputPath(),
                    getConf())) {
                RecommendJob job = new RecommendJob();
                ToolRunner.run(job, new String[] {
                        "--input", new Path(getUUUIPath(), "rowVector").toString(),
                        "--output", getOutputPath().toString() 
                });
            }
        }
        
        
        return 0;
    }
    
    private Path getUUUIPath() {
        return new Path(DeliciousDataConfig.getXiefengDir(), "uuui");
    }
}
