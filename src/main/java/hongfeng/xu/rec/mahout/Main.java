/**
 * 2013-4-2
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout;

import hongfeng.xu.rec.mahout.config.DataSetConfig;
import hongfeng.xu.rec.mahout.hadoop.BaseJob;
import hongfeng.xu.rec.mahout.hadoop.parser.DrawRawData;
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
        
        new DrawRawData(DataSetConfig.inputTraining, DataSetConfig.getTrainingDataPath(),
                "rawData-training", getConf()).draw("img/others/rawData-training.png");
        new DrawRawData(DataSetConfig.inputTest, DataSetConfig.getTestDataPath(),
                "rawData-test", getConf()).draw("img/others/rawData-test.png");
        
        return 0;
    }
    
    private void parseRawData() throws Exception {
        
        boolean toOneZero = true;
        Path output = DataSetConfig.getRawDataPath();
        RawDataParser parser = new RawDataParser(DataSetConfig.inputAll, DataSetConfig.inputTraining,
                DataSetConfig.inputTest, toOneZero);
        runJob(parser, DataSetConfig.inputAll, output, false);
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
