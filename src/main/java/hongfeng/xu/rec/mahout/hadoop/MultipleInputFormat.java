/**
 * 2013-3-21
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;

/**
 * @author xuhongfeng
 *
 */
public class MultipleInputFormat<K, V> extends FileInputFormat<K, V> {

    @Override
    public RecordReader<K, V> createRecordReader(InputSplit split,
            TaskAttemptContext context) throws IOException,
            InterruptedException {
        return new SequenceFileRecordReader<K, V>();
    }

     @Override
     protected long getFormatMinSplitSize() {
         return SequenceFile.SYNC_INTERVAL;
     }

    @Override
    protected List<FileStatus> listStatus(JobContext job) throws IOException {
        List<FileStatus> list = super.listStatus(job);
        Iterator<FileStatus> iterator = list.iterator();
        while (iterator.hasNext()) {
            if (iterator.next().getPath().getName().startsWith("_")) {
                iterator.remove();
            }
        }
        return list;
    }
}
