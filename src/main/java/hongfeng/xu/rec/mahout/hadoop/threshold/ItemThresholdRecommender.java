/**
 * 2013-3-31
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
public class ItemThresholdRecommender extends BaseRecommender {
    private final int threshold;

    public ItemThresholdRecommender(int threshold) {
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
        Path similarityPath = MovielensDataConfig.getItemSimilarityPath();
        Path itemUserVectorPath = MovielensDataConfig.getItemUserVectorPath();
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            if (!HadoopHelper.isFileExists(similarityPath, getConf())) {
                CosineSimilarityJob job = new CosineSimilarityJob(itemCount,
                        userCount, itemCount, itemUserVectorPath);
                ToolRunner.run(job, new String[] {
                        "--input", itemUserVectorPath.toString(),
                        "--output", similarityPath.toString()
                });
            }
        }
        
        /* average similarity */
        Path similarityAveragePath = MovielensDataConfig.getIIIISimilarityAverage();
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            if (!HadoopHelper.isFileExists(similarityAveragePath, getConf())) {
                int n1 = itemCount;
                int n2 = n1;
                int n3 = n1;
                Path iiSimilarityPath = new Path(similarityPath, "rowVector");
                MultiplyMatrixAverageJob job = new MultiplyMatrixAverageJob(n1, n2, n3, iiSimilarityPath);
                runJob(job, new String[] {}, iiSimilarityPath, similarityAveragePath);
            }
        }
        
        /* threshold similarity */
        Path iiThresholdPath = MovielensDataConfig.getIIThresholdPath(threshold);
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            if (!HadoopHelper.isFileExists(iiThresholdPath, getConf())) {
                int n1 = userCount;
                int n2 = itemCount;
                int n3 = n1;
                MultiplyThresholdMatrixJob job = new MultiplyThresholdMatrixJob(n1,
                        n2, n3, itemUserVectorPath, n2, similarityAveragePath, itemCount);
                runJob(job, new String[] {}, itemUserVectorPath, iiThresholdPath);
            }
        }
        
        Path uiiiThresholdPath = MovielensDataConfig.getUUUIThresholdPath(threshold);
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            if (!HadoopHelper.isFileExists(uiiiThresholdPath, getConf())) {
                int n1 = userCount;
                int n2 = itemCount;
                int n3 = itemCount;
                int type = MultiplyNearestNeighborJob.TYPE_SECOND;
                int k = 50;
                MultiplyNearestNeighborJob job = new MultiplyNearestNeighborJob(n1,
                        n2, n3, new Path(iiThresholdPath, "rowPath"), type, k);
                runJob(job, new String[] {}, MovielensDataConfig.getUserItemVectorPath(), uiiiThresholdPath);
            }
        }
        
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            if (!HadoopHelper.isFileExists(getOutputPath(),
                    getConf())) {
                RecommendJob job = new RecommendJob();
                ToolRunner.run(job, new String[] {
                        "--input", new Path(uiiiThresholdPath, "rowVector").toString(),
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