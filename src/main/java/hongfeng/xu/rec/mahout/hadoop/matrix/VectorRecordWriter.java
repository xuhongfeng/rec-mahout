/**
 * 2013-3-17
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.matrix;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.mahout.math.VectorWritable;

/**
 * @author xuhongfeng
 *
 */
public class VectorRecordWriter extends RecordWriter<KeyType, VectorWritable> {
    private final SequenceFile.Writer rowWriter;
    private final SequenceFile.Writer columnWriter;
    
    private final IntWritable intWritable = new IntWritable();

    public VectorRecordWriter(Writer rowWriter, Writer columnWriter) {
        super();
        this.rowWriter = rowWriter;
        this.columnWriter = columnWriter;
    }

    @Override
    public void write(KeyType key, VectorWritable value) throws IOException,
            InterruptedException {
        intWritable.set(key.getIndex());
        getWriter(key).append(intWritable, value);
    }
    
    private SequenceFile.Writer getWriter(KeyType key) {
        if (key.getType() == KeyType.TYPE_ROW) {
            return rowWriter;
        } else if (key.getType() == KeyType.TYPE_COLUMN) {
            return columnWriter;
        }
        throw new RuntimeException();
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException,
            InterruptedException {
        rowWriter.close();
        columnWriter.close();
    }

}
