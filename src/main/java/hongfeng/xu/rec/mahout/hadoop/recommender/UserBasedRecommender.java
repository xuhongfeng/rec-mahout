/**
 * 2013-3-24
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.recommender;

import hongfeng.xu.rec.mahout.config.DataSetConfig;
import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;
import hongfeng.xu.rec.mahout.hadoop.matrix.MultiplyNearestNeighborJob;
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
        
        int itemCount = HadoopUtil.readInt(DataSetConfig.getItemCountPath(), getConf());
        int userCount = HadoopUtil.readInt(DataSetConfig.getUserCountPath(), getConf());
        
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            if (!HadoopHelper.isFileExists(DataSetConfig.getUserSimilarityPath(), getConf())) {
                CosineSimilarityJob job = new CosineSimilarityJob(userCount,
                        itemCount, userCount, DataSetConfig.getUserItemVectorPath());
                ToolRunner.run(job, new String[] {
                        "--input", DataSetConfig.getUserItemVectorPath().toString(),
                        "--output", DataSetConfig.getUserSimilarityPath().toString()
                });
            }
        }
        
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            if (!HadoopHelper.isFileExists(DataSetConfig.getUserBasedMatrix(), getConf())) {
                int n1 = userCount;
                int n2 = n1;
                int n3 = itemCount;
                int type = MultiplyNearestNeighborJob.TYPE_FIRST;
                int k = 50;
                Path multipyerPath = DataSetConfig.getItemUserVectorPath();
                MultiplyNearestNeighborJob job = new MultiplyNearestNeighborJob(n1,
                        n2, n3, multipyerPath, type, k);
                ToolRunner.run(job, new String[] {
                        "--input", new Path(DataSetConfig.getUserSimilarityPath(), "rowVector").toString(),
                        "--output", DataSetConfig.getUserBasedMatrix().toString()
                });
            }
        }
        
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            if (!HadoopHelper.isFileExists(getOutputPath(),
                    getConf())) {
                RecommendJob job = new RecommendJob();
                ToolRunner.run(job, new String[] {
                        "--input", new Path(DataSetConfig.getUserBasedMatrix(), "rowVector").toString(),
                        "--output", getOutputPath().toString() 
                });
            }
        }
        return 0;
    }

}
