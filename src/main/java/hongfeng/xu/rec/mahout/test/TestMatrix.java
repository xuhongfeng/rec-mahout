/**
 * 2013-3-30
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.test;

import hongfeng.xu.rec.mahout.config.DataSetConfig;
import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;
import hongfeng.xu.rec.mahout.hadoop.MultipleSequenceOutputFormat;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterator;
import org.apache.mahout.math.VectorWritable;

/**
 * @author xuhongfeng
 *
 */
public class TestMatrix extends AbstractJob {

    @Override
    public int run(String[] args) throws Exception {
        testItemUserVector();
        testItemOneZeroCount();
        return 0;
    }

    public static void main(String[] args) {
        try {
            ToolRunner.run(new TestMatrix(), new String[] {
                "--input", "test",
                "--output", "test"
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private void testUserVectorCount() throws IOException {
        Path path = DataSetConfig.getUserItemVectorPath();
        SequenceFileDirIterator<IntWritable, VectorWritable> iterator
            = HadoopHelper.openVectorIterator(path, getConf());
        int count = 0;
        while (iterator.hasNext()) {
            count++;
            iterator.next();
        }
        HadoopHelper.log(this, "count=" + count);
        if (count != 943) {
            assertFailed("count = " + count);
        }
        iterator.close();
    }
    
    private void assertFailed(String msg) {
        throw new RuntimeException(msg);
    }
    
    private void testItemUserVector() throws IOException {
        int itemCount = HadoopUtil.readInt(DataSetConfig.getItemCountPath(), getConf());
        SequenceFileDirIterator<IntWritable, VectorWritable> iterator = HadoopHelper.openVectorIterator(DataSetConfig.getItemUserVectorPath(), getConf());
        int count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        if (count != itemCount) {
            assertFailed("count = " + count);
        }
        iterator.close();
    }
    
    private void testItemOneZeroCount() throws IOException {
        int itemCount = HadoopUtil.readInt(DataSetConfig.getItemCountPath(), getConf());
        HadoopHelper.log(this, "itemCount=" + itemCount);
        if (itemCount != 1682) {
            assertFailed("itemCount = " + itemCount);
        }
        
        int count = (itemCount-1)*itemCount/2;
        
        
        SequenceFileDirIterator<IntWritable, IntWritable> iterator = 
                new SequenceFileDirIterator<IntWritable, IntWritable>(DataSetConfig.getCountIUUIOneZeroPath(),
                PathType.LIST, MultipleSequenceOutputFormat.FILTER, null, true,
                getConf());
        int c = 0;
        while (iterator.hasNext()) {
            Pair<IntWritable, IntWritable> pair = iterator.next();
            c += pair.getSecond().get();
        }
        if (c != count) {
            assertFailed("c = " + c);
        }
        iterator.close();
    }
}
