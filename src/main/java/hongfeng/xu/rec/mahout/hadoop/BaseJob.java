/**
 * 2013-4-3
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop;

import hongfeng.xu.rec.mahout.config.DataSetConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterator;

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
    
    private int itemCount = -1;
    protected int itemCount() {
        if (itemCount == -1) {
            try {
                itemCount = HadoopUtil.readInt(DataSetConfig.getItemCountPath(), getConf());
            } catch (IOException e) {
                throw new RuntimeException();
            }
        }
        return itemCount;
    }
    
    private int userCount = -1;
    protected int userCount() {
        if (userCount == -1) {
            try {
                userCount = HadoopUtil.readInt(DataSetConfig.getUserCountPath(), getConf());
            } catch (IOException e) {
                throw new RuntimeException();
            }
        }
        return userCount;
    }
    
    protected abstract int innerRun() throws Exception;
    
    protected JobQueue createJobQueue() {
        JobQueue queue = new JobQueue();
        return queue;
    }
    
    public class JobQueue {
        private final List<Job> jobs = new ArrayList<Job>();
        
        public void submitJob(Job job) throws Exception {
            jobs.add(job);
            job.submit();
        }
        
        public int waitForComplete() throws Exception {
            while (jobs.size() > 0) {
                Iterator<Job> iterator = jobs.iterator();
                while (iterator.hasNext()) {
                    Job job = iterator.next();
                    if (job.isComplete()) {
                        if (!job.isSuccessful()) {
                            return -1;
                        }
                        iterator.remove();
                    }
                }
                Thread.sleep(1000L);
            }
            return 0;
        }
    }
    
    protected <K extends Writable, V extends Writable> SequenceFileDirIterator<K, V> open(Class<K> keyClass
            ,Class<V> valueClass, Path path) throws IOException {
        return new SequenceFileDirIterator<K, V>(path, PathType.LIST,
                MultipleSequenceOutputFormat.FILTER, null, true, getConf());
    }
}
