/**
 * 2013-3-31
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
public class ItemBasedRecommender extends BaseRecommender {

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
        
        Path itemSimilarityPath = DataSetConfig.getItemSimilarityPath();
        Path itemUserVectorPath = DataSetConfig.getItemUserVectorPath();
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            if (!HadoopHelper.isFileExists(itemSimilarityPath, getConf())) {
                CosineSimilarityJob job = new CosineSimilarityJob(itemCount,
                        userCount, itemCount, itemUserVectorPath);
                ToolRunner.run(job, new String[] {
                        "--input", itemUserVectorPath.toString(),
                        "--output", itemSimilarityPath.toString()
                });
            }
        }
        
        Path itemBasedMatrixPath = DataSetConfig.getItemBasedMatrix();
        Path userItemVectorPath = DataSetConfig.getUserItemVectorPath();
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            if (!HadoopHelper.isFileExists(itemBasedMatrixPath, getConf())) {
                int n1 = userCount;
                int n2 = itemCount;
                int n3 = itemCount;
                int type = MultiplyNearestNeighborJob.TYPE_SECOND;
                int k = 50;
                Path multipyerPath = new Path(itemSimilarityPath, "rowVector");
                MultiplyNearestNeighborJob job = new MultiplyNearestNeighborJob(n1,
                        n2, n3, multipyerPath, type, k);
                ToolRunner.run(job, new String[] {
                        "--input", userItemVectorPath.toString(),
                        "--output", itemBasedMatrixPath.toString()
                });
            }
        }
        
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            if (!HadoopHelper.isFileExists(getOutputPath(),
                    getConf())) {
                RecommendJob job = new RecommendJob();
                ToolRunner.run(job, new String[] {
                        "--input", new Path(itemBasedMatrixPath, "rowVector").toString(),
                        "--output", getOutputPath().toString() 
                });
            }
        }
        
        return 0;
    }

}
