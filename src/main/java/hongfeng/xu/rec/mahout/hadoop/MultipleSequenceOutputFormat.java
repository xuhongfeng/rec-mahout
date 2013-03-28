/**
 * 2013-3-19
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * @author xuhongfeng
 *
 */
public class MultipleSequenceOutputFormat<K, V> extends MultipleOutputFormat<K, V> {
    public static final PathFilter FILTER = new PathFilter() {
        @Override
        public boolean accept(Path path) {
            return !path.getName().startsWith("_");
        }
    };
    @SuppressWarnings("rawtypes")
    @Override
    protected RecordWriter<K, V> getBaseWriter(TaskAttemptContext context, Path path,
            Class keyClass, Class valClass) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        Writer w = null;
        while (w == null) {
            try {
                w = SequenceFile.createWriter(fs, conf, path, keyClass, valClass);
            } catch (Throwable e) {
                path = new Path(path.getParent(), String.valueOf(new Random().nextInt(Integer.MAX_VALUE)));
            }
        }
        final Writer writer = w;
        return new RecordWriter<K, V>() {

            @Override
            public void write(K key, V value) throws IOException,
                    InterruptedException {
                writer.append(key, value);
            }

            @Override
            public void close(TaskAttemptContext context) throws IOException,
                    InterruptedException {
                writer.close();
            }};
    }
}
