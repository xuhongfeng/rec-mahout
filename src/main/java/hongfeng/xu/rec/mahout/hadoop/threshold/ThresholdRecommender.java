/**
 * 2013-3-29
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.threshold;

import hongfeng.xu.rec.mahout.config.MovielensDataConfig;
import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;
import hongfeng.xu.rec.mahout.hadoop.matrix.MultiplyMatrixAverageJob;
import hongfeng.xu.rec.mahout.hadoop.matrix.MultiplyNearestNeighborJob;
import hongfeng.xu.rec.mahout.hadoop.recommender.BaseRecommender;
import hongfeng.xu.rec.mahout.hadoop.recommender.RecommendJob;
import hongfeng.xu.rec.mahout.hadoop.similarity.CosineSimilarityJob;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.HadoopUtil;

/**
 * @author xuhongfeng
 *
 */
public class ThresholdRecommender extends BaseRecommender {
    private final int threshold;

    public ThresholdRecommender(int threshold) {
        super();
        this.threshold = threshold;
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
        
        int itemCount = HadoopUtil.readInt(MovielensDataConfig.getItemCountPath(), getConf());
        int userCount = HadoopUtil.readInt(MovielensDataConfig.getUserCountPath(), getConf());
        
        /* similarity */
        Path userItemVectorPath = MovielensDataConfig.getUserItemVectorPath();
        Path similarityPath = MovielensDataConfig.getUserSimilarityPath();
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            if (!HadoopHelper.isFileExists(similarityPath, getConf())) {
                CosineSimilarityJob job = new CosineSimilarityJob(userCount,
                        itemCount, userCount, userItemVectorPath);
                ToolRunner.run(job, new String[] {
                        "--input", userItemVectorPath.toString(),
                        "--output", similarityPath.toString()
                });
            }
        }
        
        /* average similarity */
        Path similarityAveragePath = MovielensDataConfig.getUUUUSimilarityAverage();
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            if (!HadoopHelper.isFileExists(similarityAveragePath, getConf())) {
                int n1 = userCount;
                int n2 = n1;
                int n3 = n1;
                Path uuSimilarityPath = new Path(similarityPath, "rowVector");
                MultiplyMatrixAverageJob job = new MultiplyMatrixAverageJob(n1, n2, n3, uuSimilarityPath);
                runJob(job, new String[] {}, uuSimilarityPath, similarityAveragePath);
            }
        }
        
        /* threshold similarity */
        Path uuThresholdPath = MovielensDataConfig.getUUThresholdPath(threshold);
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            if (!HadoopHelper.isFileExists(uuThresholdPath, getConf())) {
                int n1 = userCount;
                int n2 = itemCount;
                int n3 = n1;
                MultiplyThresholdMatrixJob job = new MultiplyThresholdMatrixJob(n1, n2, n3, userItemVectorPath
                        ,threshold);
                runJob(job, new String[] {}, userItemVectorPath, MovielensDataConfig.getUUThresholdPath(threshold));
            }
        }
        
        Path uuuiThresholdPath = MovielensDataConfig.getUUUIThresholdPath(threshold);
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            if (!HadoopHelper.isFileExists(uuuiThresholdPath, getConf())) {
                int n1 = userCount;
                int n2 = n1;
                int n3 = itemCount;
                int type = MultiplyNearestNeighborJob.TYPE_FIRST;
                int k = 50;
                MultiplyNearestNeighborJob job = new MultiplyNearestNeighborJob(n1,
                        n2, n3, userItemVectorPath, type, k);
                runJob(job, new String[] {}, new Path(uuThresholdPath, "rowVector"), uuuiThresholdPath);
            }
        }
        
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            if (!HadoopHelper.isFileExists(getOutputPath(),
                    getConf())) {
                RecommendJob job = new RecommendJob();
                ToolRunner.run(job, new String[] {
                        "--input", new Path(uuuiThresholdPath, "rowVector").toString(),
                        "--output", getOutputPath().toString() 
                });
            }
        }
        return 0;
    }
    
    private void runJob (Tool job, String[] args, Path input, Path output) throws Exception {
        if (!HadoopHelper.isFileExists(output, getConf())) {
            args = (String[]) ArrayUtils.addAll(new String[] {
                "--input", input.toString(),
                "--output", output.toString(),
            }, args);
            ToolRunner.run(getConf(), job, args);
        }
    }

}
