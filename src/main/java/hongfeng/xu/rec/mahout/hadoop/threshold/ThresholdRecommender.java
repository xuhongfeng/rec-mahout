/**
 * 2013-3-29
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.threshold;

import hongfeng.xu.rec.mahout.config.MovielensDataConfig;
import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;
import hongfeng.xu.rec.mahout.hadoop.matrix.MultiplyMatrixAverageJob;
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
            if (!HadoopHelper.isFileExists(MovielensDataConfig.getUserCosineSimilarityPath(), getConf())) {
                int itemCount = HadoopUtil.readInt(MovielensDataConfig.getItemCountPath(), getConf());
                int userCount = HadoopUtil.readInt(MovielensDataConfig.getUserCountPath(), getConf());
                CosineSimilarityJob job = new CosineSimilarityJob(userCount,
                        itemCount, userCount, MovielensDataConfig.getUserItemVectorPath());
                ToolRunner.run(job, new String[] {
                        "--input", MovielensDataConfig.getUserItemVectorPath().toString(),
                        "--output", MovielensDataConfig.getUserCosineSimilarityPath().toString()
                });
            }
        }
        
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            int n1 = HadoopUtil.readInt(MovielensDataConfig.getUserCountPath(), getConf());
            int n2 = n1;
            int n3 = n1;
            Path uuCosinePath = new Path(MovielensDataConfig.getUserCosineSimilarityPath(), "rowVector");
            MultiplyMatrixAverageJob job = new MultiplyMatrixAverageJob(n1, n2, n3, uuCosinePath);
            runJob(job, new String[] {}, uuCosinePath, MovielensDataConfig.getUUUUCosineAverage());
        }
        
        int threshold = 30;
        Path uuThresholdPath = new Path(MovielensDataConfig.getUUThresholdPath(), String.valueOf(threshold));
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            int n1 = HadoopUtil.readInt(MovielensDataConfig.getUserCountPath(), getConf());
            int n2 = HadoopUtil.readInt(MovielensDataConfig.getItemCountPath(), getConf());
            int n3 = n1;
            Path userItemVectorPath = MovielensDataConfig.getUserItemVectorPath();
            MultiplyThresholdMatrixJob job = new MultiplyThresholdMatrixJob(n1, n2, n3, userItemVectorPath
                    ,30);
            runJob(job, new String[] {}, userItemVectorPath, MovielensDataConfig.getUUThresholdPath());
        }
        
        Path uuuiThresholdPath = new Path(MovielensDataConfig.getUUUIThresholdPath(), String.valueOf(threshold));
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            int n1 = HadoopUtil.readInt(MovielensDataConfig.getUserCountPath(), getConf());
            int n2 = n1;
            int n3 = HadoopUtil.readInt(MovielensDataConfig.getItemCountPath(), getConf());
            MultiplyMatrixAverageJob job = new MultiplyMatrixAverageJob(n1, n2, n3, MovielensDataConfig.getUserItemVectorPath());
            runJob(job, new String[] {}, new Path(uuThresholdPath, "rowVector"), uuuiThresholdPath);
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
