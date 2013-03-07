/**
 * 2013-3-10
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.parser;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * @author xuhongfeng
 *
 */
public class ParserRecordWriter extends RecordWriter<KeyType, DoubleWritable> {
    private final FSDataOutputStream userItemOutput;
    private final FSDataOutputStream userTagOutput;
    private final FSDataOutputStream itemTagOutput;

    public ParserRecordWriter(FSDataOutputStream userItemOutput,
            FSDataOutputStream userTagOutput, FSDataOutputStream itemTagOutput) {
        super();
        this.userItemOutput = userItemOutput;
        this.userTagOutput = userTagOutput;
        this.itemTagOutput = itemTagOutput;
    }

    @Override
    public void write(KeyType key, DoubleWritable value) throws IOException,
            InterruptedException {
        String str = String.format("%d\t%d\t%f\n", key.getId1(), key.getId2(), value.get());
        getOutputStream(key).write(str.getBytes("UTF-8"));
    }
    
    private FSDataOutputStream getOutputStream(KeyType key) {
        if (key.getType() == KeyType.TYPE_USER_ITEM) {
            return userItemOutput;
        } else if (key.getType() == KeyType.TYPE_USER_TAG) {
            return userTagOutput;
        }
        return itemTagOutput;
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException,
            InterruptedException {
        IOUtils.closeStream(userItemOutput);
        IOUtils.closeStream(userTagOutput);
        IOUtils.closeStream(itemTagOutput);
    }

}
