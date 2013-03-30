/**
 * 2013-3-24
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.threshold;

import hongfeng.xu.rec.mahout.config.MovielensDataConfig;
import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;
import hongfeng.xu.rec.mahout.hadoop.matrix.MultiplyMatrixJob;
import hongfeng.xu.rec.mahout.hadoop.recommender.BaseRecommender;
import hongfeng.xu.rec.mahout.hadoop.recommender.RecommendJob;
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
        
        int itemCount = HadoopUtil.readInt(MovielensDataConfig.getItemCountPath(), getConf());
        int userCount = HadoopUtil.readInt(MovielensDataConfig.getUserCountPath(), getConf());
        
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            if (!HadoopHelper.isFileExists(MovielensDataConfig.getUserCosineSimilarityPath(), getConf())) {
                CosineSimilarityJob job = new CosineSimilarityJob(userCount,
                        itemCount, userCount, MovielensDataConfig.getUserItemVectorPath());
                ToolRunner.run(job, new String[] {
                        "--input", MovielensDataConfig.getUserItemVectorPath().toString(),
                        "--output", MovielensDataConfig.getUserCosineSimilarityPath().toString()
                });
            }
        }
        
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            if (!HadoopHelper.isFileExists(MovielensDataConfig.getUserBasedMatrix(), getConf())) {
                int n1 = userCount;
                int n2 = n1;
                int n3 = itemCount;
                Path multipyerPath = MovielensDataConfig.getItemUserVectorPath();
                MultiplyMatrixJob job = new MultiplyMatrixJob(n1, n2, n3, multipyerPath);
                ToolRunner.run(job, new String[] {
                        "--input", new Path(MovielensDataConfig.getUserCosineSimilarityPath(), "rowVector").toString(),
                        "--output", MovielensDataConfig.getUserBasedMatrix().toString()
                });
            }
        }
        
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            if (!HadoopHelper.isFileExists(getOutputPath(),
                    getConf())) {
                RecommendJob job = new RecommendJob();
                ToolRunner.run(job, new String[] {
                        "--input", new Path(MovielensDataConfig.getUserBasedMatrix(), "rowVector").toString(),
                        "--output", getOutputPath().toString() 
                });
            }
        }
        return 0;
    }

}
