/**
 * 2013-3-26
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.neighbor;

import hongfeng.xu.rec.mahout.config.MovielensDataConfig;
import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;
import hongfeng.xu.rec.mahout.hadoop.matrix.MultiplyMatrixJob;
import hongfeng.xu.rec.mahout.util.L;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;

/**
 * @author xuhongfeng
 *
 */
public class Main extends AbstractJob {
    
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
            RawDataParser job = new RawDataParser();
            runJob(job, new String[] {}, getInputPath(),
                MovielensDataConfig.getRawDataPath());
        }
        
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            ToVectorJob job = new ToVectorJob();
            runJob(job, new String[] {}, MovielensDataConfig.getRawDataPath(),
                    MovielensDataConfig.getMatrixPath());
        }
        
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            int n1 = HadoopUtil.readInt(MovielensDataConfig.getUserCountPath(), getConf());
            int n2 = HadoopUtil.readInt(MovielensDataConfig.getItemCountPath(), getConf());
            int n3 = n1;
            Path multipyerPath = MovielensDataConfig.getUserItemOneZeroVectorPath();
            MultiplyMatrixJob job = new MultiplyMatrixJob(n1, n2, n3, multipyerPath);
            runJob(job, new String[] {}, MovielensDataConfig.getUserItemOneZeroVectorPath(),
                    MovielensDataConfig.getUIIUOneZero());
        }
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            CountUIIUOneZeroJob job = new CountUIIUOneZeroJob();
            Path input = new Path(MovielensDataConfig.getUIIUOneZero(), "rowVector");
            Path output = MovielensDataConfig.getCountUIIUOneZeroPath();
            runJob(job, new String[] {}, input, output);
        }
        
        new DrawCountUUOneZero().draw(getConf());
        
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


    public static void main(String[] args) {
        Main job = new Main();
        try {
            ToolRunner.run(job, args);
        } catch (Exception e) {
            L.e(job, e);
        }
    }

}
