/**
 * 2013-3-24
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.recommender;

import hongfeng.xu.rec.mahout.config.DeliciousDataConfig;
import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;
import hongfeng.xu.rec.mahout.hadoop.matrix.MultiplyMatrixJob;
import hongfeng.xu.rec.mahout.hadoop.similarity.CosineSimilarityJob;

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
public class UserBasedRecommender extends BaseRecommender {

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
            if (!HadoopHelper.isFileExists(DeliciousDataConfig.getUserSimilarityPath(), getConf())) {
                int itemCount = HadoopUtil.readInt(DeliciousDataConfig.getItemCountPath(), getConf());
                int userCount = HadoopUtil.readInt(DeliciousDataConfig.getUserCountPath(), getConf());
                CosineSimilarityJob job = new CosineSimilarityJob(userCount,
                        itemCount, userCount, DeliciousDataConfig.getUserItemVectorPath());
                ToolRunner.run(job, new String[] {
                        "--input", DeliciousDataConfig.getUserItemVectorPath().toString(),
                        "--output", DeliciousDataConfig.getUserSimilarityPath().toString()
                });
            }
        }
        
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            if (!HadoopHelper.isFileExists(DeliciousDataConfig.getUserBasedMatrix(), getConf())) {
                int n1 = HadoopUtil.readInt(DeliciousDataConfig.getUserCountPath(), getConf());
                int n2 = n1;
                int n3 = HadoopUtil.readInt(DeliciousDataConfig.getItemCountPath(), getConf());
                Path multipyerPath = DeliciousDataConfig.getItemUserVectorPath();
                MultiplyMatrixJob job = new MultiplyMatrixJob(n1, n2, n3, multipyerPath);
                ToolRunner.run(job, new String[] {
                        "--input", new Path(DeliciousDataConfig.getUserSimilarityPath(), "rowVector").toString(),
                        "--output", DeliciousDataConfig.getUserBasedMatrix().toString()
                });
            }
        }
        
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            if (!HadoopHelper.isFileExists(getOutputPath(),
                    getConf())) {
                RecommendJob job = new RecommendJob();
                ToolRunner.run(job, new String[] {
                        "--input", new Path(DeliciousDataConfig.getUserBasedMatrix(), "rowVector").toString(),
                        "--output", getOutputPath().toString() 
                });
            }
        }
        return 0;
    }

}
