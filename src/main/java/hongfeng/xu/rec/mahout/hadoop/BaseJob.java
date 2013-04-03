/**
 * 2013-4-3
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;

/**
 * @author xuhongfeng
 *
 */
public abstract class BaseJob extends AbstractJob {
    private Map<String,List<String>> parsedArgs;

    @Override
    public final int run(String[] args) throws Exception {
        addInputOption();
        addOutputOption();
        
        parsedArgs = parseArguments(args);
    
        initConf(getConf());
        
        return innerRun();
    }
    
    protected void runJob (Tool job, Path input, Path output,
            boolean checkOutputExist) throws Exception {
        if (!checkOutputExist || !HadoopHelper.isFileExists(output, getConf())) {
            String[] args = new String[] {
                "--input", input.toString(),
                "--output", output.toString(),
            };
            ToolRunner.run(getConf(), job, args);
        }
    }
    
    protected void initConf(Configuration conf) {
    }

    protected abstract int innerRun() throws Exception;
}
