/**
 * 2013-3-19
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author xuhongfeng
 *
 */
public abstract class MultipleOutputFormat<K, V> extends FileOutputFormat<K, V>{

    @Override
    public RecordWriter<K, V> getRecordWriter(final TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new RecordWriter<K, V>() {
            private final TreeMap<String, RecordWriter<K, V>> map = new TreeMap<String, RecordWriter<K,V>>();

            @Override
            public void write(K key, V value) throws IOException,
                    InterruptedException {
                Path outputPath = new Path(context.getConfiguration().get("mapred.output.dir"));
                Path path = getPath(outputPath, key);
                RecordWriter<K, V> writer = map.get(path.toString());
                if (writer == null) {
                    writer = getBaseWriter(context, path, key.getClass(), value.getClass());
                    map.put(path.toString(), writer);
                }
                writer.write(key, value);
            }

            @Override
            public void close(TaskAttemptContext context) throws IOException,
                    InterruptedException {
                for (RecordWriter<K, V> writer:map.values()) {
                    writer.close(context);
                }
                map.clear();
            }
        };
    }
    
    @SuppressWarnings("rawtypes")
    protected abstract RecordWriter<K, V> getBaseWriter(TaskAttemptContext context, Path path,
            Class keyClass, Class valClass) throws IOException, InterruptedException;
    
    protected Path getPath(Path outputPath, K key) {
        return new Path(outputPath, getFile(key));
    }
    
    protected String getFile(K key) {
        return key.toString();
    }
}
