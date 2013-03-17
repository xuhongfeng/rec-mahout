/**
 * 2013-3-10
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.parser;

import hongfeng.xu.rec.mahout.config.DeliciousDataConfig;
import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;

import java.io.IOException;

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
        return new ParserRecordWriter(
                context.getConfiguration(),
                HadoopHelper.createFile(DeliciousDataConfig.getUserItemPath(), context.getConfiguration()),
                HadoopHelper.createFile(DeliciousDataConfig.getUserTagPath(), context.getConfiguration()),
                HadoopHelper.createFile(DeliciousDataConfig.getItemTagPath(), context.getConfiguration()),
                HadoopHelper.createFile(DeliciousDataConfig.getTestDataPath(), context.getConfiguration())
                );
    }

}
