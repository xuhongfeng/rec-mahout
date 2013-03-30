/**
 * 2013-3-30
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.test;

import hongfeng.xu.rec.mahout.config.MovielensDataConfig;
import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;
import hongfeng.xu.rec.mahout.hadoop.MultipleSequenceOutputFormat;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterator;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * @author xuhongfeng
 *
 */
public class TestMatrix extends AbstractJob {

    @Override
    public int run(String[] args) throws Exception {
//        testUserVectorCount();
//        testUserSimilarityDistribution();
        testUserSimilarity();
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
    
    private void testUserSimilarityDistribution () throws IOException {
        Path path = new Path(MovielensDataConfig.getUserCosineSimilarityPath()
                , "distribution");
        SequenceFileDirIterator<DoubleWritable, IntWritable> iterator
            = new SequenceFileDirIterator<DoubleWritable, IntWritable>(path, PathType.LIST,
                    MultipleSequenceOutputFormat.FILTER, null, true, getConf());
        int count = 0;
        while (iterator.hasNext()) {
            Pair<DoubleWritable, IntWritable> pair = iterator.next();
            double v = pair.getFirst().get();
            int n = pair.getSecond().get();
            HadoopHelper.log(this, "v=" + v + ", n=" + n);
            count += n;
        }
        HadoopHelper.log(this, "count=" + count);
        iterator.close();
    }
    
    private void testUserSimilarity() throws IOException {
        Path path = new Path(MovielensDataConfig.getUserCosineSimilarityPath(), "rowVector");
        SequenceFileDirIterator<IntWritable, VectorWritable> iterator
            = HadoopHelper.openVectorIterator(path, getConf());
        
//        path = new Path(path, "0");
//        SequenceFileIterator<IntWritable, VectorWritable> iterator = new SequenceFileIterator<IntWritable, VectorWritable>(path, true, getConf());
        int count = 0;
        while (iterator.hasNext()) {
            count++;
            Pair<IntWritable, VectorWritable> pair = iterator.next();
            Vector vector = pair.getSecond().get();
            if (vector.size() != 943) {
                assertFailed("vector size = " + vector.size());
            }
        }
        HadoopHelper.log(this, "count=" + count);
        if (count != 943) {
            assertFailed("count = " + count);
        }
        iterator.close();
    }
}
