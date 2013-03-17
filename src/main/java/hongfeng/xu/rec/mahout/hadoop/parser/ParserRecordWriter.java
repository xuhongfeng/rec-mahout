/**
 * 2013-3-10
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.parser;

import hongfeng.xu.rec.mahout.hadoop.misc.BaseIndexMap.IndexType;
import hongfeng.xu.rec.mahout.hadoop.misc.IdIndexMap;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
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
    private final FSDataOutputStream testOutput;
    
    private IdIndexMap userIndexMap;
    private IdIndexMap itemIndexMap;
    private IdIndexMap tagIndexMap;
    
    private final Configuration conf;
    
    public ParserRecordWriter(Configuration conf, FSDataOutputStream userItemOutput,
            FSDataOutputStream userTagOutput, FSDataOutputStream itemTagOutput
            , FSDataOutputStream testOutput) {
        super();
        this.conf = conf;
        this.userItemOutput = userItemOutput;
        this.userTagOutput = userTagOutput;
        this.itemTagOutput = itemTagOutput;
        this.testOutput = testOutput;
    }

    @Override
    public void write(KeyType key, DoubleWritable value) throws IOException,
            InterruptedException {
        int id1 = 0, id2 = 0;
        if (key.getType() == KeyType.TYPE_USER_ITEM) {
            id1 = getUserIndex(key.getId1());
            id2 = getItemIndex(key.getId2());
        } else if (key.getType() == KeyType.TYPE_USER_TAG) {
            id1 = getUserIndex(key.getId1());
            id2 = getTagIndex(key.getId2());
        } else if (key.getType() == KeyType.TYPE_ITEM_TAG) {
            id1 = getItemIndex(key.getId1());
            id2 = getTagIndex(key.getId2());
        } else if (key.getType() == KeyType.TYPE_TEST_DATA) {
            id1 = getUserIndex(key.getId1());
            id2 = getItemIndex(key.getId2());
        }
        String str = String.format("%d\t%d\t%f\n", id1, id2, value.get());
        getOutputStream(key).write(str.getBytes("UTF-8"));
    }
    
    private int getUserIndex(long id) throws IOException {
        if (userIndexMap == null) {
            userIndexMap = IdIndexMap.create(IndexType.UserIndex, conf);
        }
        return userIndexMap.getIndex(id);
    }
    
    private int getItemIndex(long id) throws IOException {
        if (itemIndexMap == null) {
            itemIndexMap = IdIndexMap.create(IndexType.ItemIndex, conf);
        }
        return itemIndexMap.getIndex(id);
    }
    
    private int getTagIndex(long id) throws IOException {
        if (tagIndexMap == null) {
            tagIndexMap = IdIndexMap.create(IndexType.TagIndex, conf);
        }
        return tagIndexMap.getIndex(id);
    }
    
    private FSDataOutputStream getOutputStream(KeyType key) {
        if (key.getType() == KeyType.TYPE_USER_ITEM) {
            return userItemOutput;
        } else if (key.getType() == KeyType.TYPE_USER_TAG) {
            return userTagOutput;
        } else if (key.getType() == KeyType.TYPE_ITEM_TAG) {
            return itemTagOutput;
        }
        return testOutput;
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException,
            InterruptedException {
        IOUtils.closeStream(userItemOutput);
        IOUtils.closeStream(userTagOutput);
        IOUtils.closeStream(itemTagOutput);
        IOUtils.closeStream(testOutput);
    }

}
