/**
 * 2013-4-2
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout;

import hongfeng.xu.rec.mahout.config.DataSetConfig;
import hongfeng.xu.rec.mahout.hadoop.BaseJob;
import hongfeng.xu.rec.mahout.hadoop.matrix.ToVectorJob;
import hongfeng.xu.rec.mahout.hadoop.parser.RawDataParser;
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
        
        return 0;
    }
    
    private void toVector() throws Exception {
        ToVectorJob job = new ToVectorJob();
        runJob(job, DataSetConfig.getRawDataPath(), DataSetConfig.getMatrixPath(), false);
        
        //draw distribution
//        int mode = DrawMatrixJob.MODE_WITH_ZERO;
//        float precesion = 0.001f;
//        String imageFile = "img/others/distribution_origin_matrix.png";
//        String title = "origin matrix distribution";
//        DrawMatrixJob job = new DrawMatrixJob(mode, precesion, imageFile, title,
//                new String[0], new Path[] {DataSetConfig.getUserItemMatrixPath()},
//                new String[] {""});
//        runJob(job, DataSetConfig.getUserItemMatrixPath(), new Path(DataSetConfig.getUserItemMatrixPath(),
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
    
    public static void main(String[] args) {
        Main job = new Main();
        try {
            ToolRunner.run(job, args);
        } catch (Exception e) {
            L.e(job, e);
        }
    }
}
