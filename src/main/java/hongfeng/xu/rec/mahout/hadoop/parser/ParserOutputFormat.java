/**
 * 2013-3-10
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.parser;

import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author xuhongfeng
 *
 */
public class ParserOutputFormat extends FileOutputFormat<KeyType, DoubleWritable> {
    
    public RecordWriter<KeyType, DoubleWritable> getRecordWriter(
            TaskAttemptContext context) throws IOException,
            InterruptedException {
        Path dir = getOutputPath(context);
        return new ParserRecordWriter(
                HadoopHelper.createFile(new Path(dir, "user-item"), context.getConfiguration()),
                HadoopHelper.createFile(new Path(dir, "user-tag"), context.getConfiguration()),
                HadoopHelper.createFile(new Path(dir, "item-tag"), context.getConfiguration()),
                HadoopHelper.createFile(new Path(dir, "test-data"), context.getConfiguration())
                );
    }

}
