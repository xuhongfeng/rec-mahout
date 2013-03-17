/**
 * 2013-3-17
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.matrix;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.mahout.math.VectorWritable;

/**
 * @author xuhongfeng
 *
 */
public abstract class VectorOutputFormat extends FileOutputFormat<KeyType, VectorWritable> {

    @Override
    public RecordWriter<KeyType, VectorWritable> getRecordWriter(
            TaskAttemptContext job) throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(job.getConfiguration());
        SequenceFile.Writer rowWriter = SequenceFile.createWriter(fs, job.getConfiguration(),
                getRowVectorPath(), IntWritable.class, VectorWritable.class);
        SequenceFile.Writer columnWriter = SequenceFile.createWriter(fs, job.getConfiguration(),
                getColumnVectorPath(), IntWritable.class, VectorWritable.class);
        return new VectorRecordWriter(rowWriter, columnWriter);
    }

    protected abstract Path getRowVectorPath();
    protected abstract Path getColumnVectorPath();
}
