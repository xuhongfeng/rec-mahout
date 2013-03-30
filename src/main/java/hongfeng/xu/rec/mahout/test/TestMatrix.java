/**
 * 2013-3-30
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.test;

import hongfeng.xu.rec.mahout.config.MovielensDataConfig;
import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterator;
import org.apache.mahout.math.VectorWritable;

/**
 * @author xuhongfeng
 *
 */
public class TestMatrix extends AbstractJob {

    @Override
    public int run(String[] args) throws Exception {
//        testUserVectorCount();
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
        Path path = MovielensDataConfig.getUserItemVectorPath();
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
    
}
