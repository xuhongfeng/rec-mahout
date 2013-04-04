/**
 * 2013-4-2
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout;

import hongfeng.xu.rec.mahout.config.DataSetConfig;
import hongfeng.xu.rec.mahout.hadoop.BaseJob;
import hongfeng.xu.rec.mahout.hadoop.matrix.DrawMatrixJob;
import hongfeng.xu.rec.mahout.hadoop.matrix.MultiplyMatrixAverageJob;
import hongfeng.xu.rec.mahout.hadoop.matrix.ToVectorJob;
import hongfeng.xu.rec.mahout.hadoop.parser.RawDataParser;
import hongfeng.xu.rec.mahout.hadoop.similarity.CosineSimilarityJob;
import hongfeng.xu.rec.mahout.hadoop.similarity.ThresholdCosineSimilarityJob;
import hongfeng.xu.rec.mahout.hadoop.threshold.MultiplyThresholdMatrixJob;
import hongfeng.xu.rec.mahout.util.L;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author xuhongfeng
 *
 */
public class Main extends BaseJob {

    @Override
    protected int innerRun() throws Exception {
        parseRawData();
        
        toVector();
        
        calculateSimilarity();
        
        return 0;
    }
    
    private void toVector() throws Exception {
        ToVectorJob job = new ToVectorJob();
        runJob(job, DataSetConfig.getRawDataPath(), DataSetConfig.getMatrixPath(), false);
        
        //draw distribution
//        float precesion = 0.001f;
//        String imageFile = "img/others/distribution_origin_matrix.png";
//        String title = "origin matrix distribution";
//        DrawMatrixJob drawJob = new DrawMatrixJob(precesion, imageFile, title,
//                new String[0], new Path[] {DataSetConfig.getUserItemMatrixPath()},
//                new String[] {""}, true, false);
//        runJob(drawJob, DataSetConfig.getUserItemMatrixPath(), new Path(DataSetConfig.getUserItemMatrixPath(),
//                "distributon"), false);
    }
    
    private void parseRawData() throws Exception {
        
        boolean toOneZero = true;
        Path output = DataSetConfig.getRawDataPath();
        RawDataParser parser = new RawDataParser(DataSetConfig.inputAll, DataSetConfig.inputTraining,
                DataSetConfig.inputTest, toOneZero);
        runJob(parser, DataSetConfig.inputAll, output, false);
        
//        new DrawRawData(DataSetConfig.inputTraining, DataSetConfig.getTrainingDataPath(),
//                "rawData-training", getConf()).draw("img/others/rawData-training.png");
//        new DrawRawData(DataSetConfig.inputTest, DataSetConfig.getTestDataPath(),
//                "rawData-test", getConf()).draw("img/others/rawData-test.png");
    }
    
    private void calculateSimilarity() throws Exception {
        int n1 = userCount();
        int n2 = itemCount();
        int n3 = n1;
        int threshold = 30;
        Path multiplyerPath = DataSetConfig.getUserItemVectorPath();
        ThresholdCosineSimilarityJob thresholdCosineSimilarityJob =
                new ThresholdCosineSimilarityJob(n1, n2, n3, multiplyerPath, threshold);
        runJob(thresholdCosineSimilarityJob, DataSetConfig.getUserItemVectorPath(),
                DataSetConfig.getSimilarityThresholdPath(threshold), true);
        
        CosineSimilarityJob cosineSimilarityJob = new CosineSimilarityJob(n1, n2, n3, multiplyerPath);
        runJob(cosineSimilarityJob, DataSetConfig.getUserItemVectorPath(),
                DataSetConfig.getSimilarityPath(), true);
        
        Path similarityVectorPath = new Path(DataSetConfig.getSimilarityThresholdPath(threshold), "rowVector");
        MultiplyMatrixAverageJob matrixAverageJob = new MultiplyMatrixAverageJob(n1, n2, n3,
                similarityVectorPath);
        runJob(matrixAverageJob, similarityVectorPath, DataSetConfig.getSimilarityThresholdAveragePath(threshold)
                , true);
        
        Path averageSimilarityPath = new Path(DataSetConfig.getSimilarityThresholdAveragePath(threshold), "rowVector");
        MultiplyThresholdMatrixJob multiplyThresholdMatrixJob =
                new MultiplyThresholdMatrixJob(n1, n2, n3, DataSetConfig.getUserItemVectorPath(),
                        threshold, averageSimilarityPath);
        runJob(multiplyThresholdMatrixJob, DataSetConfig.getUserItemVectorPath(),
                DataSetConfig.getUUThresholdPath(threshold), true);
        
        float precision = 0.001f;
        String imageFile = "img/others/similarity_distribution.png";
        String title = "similarity";
        String[] subTitles = new String[0];
        Path[] matrixDirs = new Path[] {
                DataSetConfig.getSimilarityPath(),
                DataSetConfig.getSimilarityThresholdPath(threshold),
                DataSetConfig.getSimilarityThresholdAveragePath(threshold),
                DataSetConfig.getUUThresholdPath(threshold)
        };
        String[] series = new String[] {
                "origin",
                "filter-30",
                "similarity-average-30",
                "threshold-30"
        };
        boolean withZero = true;
        boolean diagonalOnly = true;
        DrawMatrixJob drawJob = new DrawMatrixJob(precision, imageFile, title, subTitles,
                matrixDirs, series, withZero, diagonalOnly);
        runJob(drawJob, new Path("test"), new Path("test"), false);
    }
    
    public static void main(String[] args) {
        Main job = new Main();
        try {
            ToolRunner.run(job, args);
        } catch (Exception e) {
            L.e(job, e);
        }
    }
}
