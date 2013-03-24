/**
 * 2013-3-10
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterator;
import org.apache.mahout.math.VectorWritable;

/**
 * @author xuhongfeng
 *
 */
public class HadoopHelper {

    public static boolean isFileExists(String path, Configuration conf) throws IOException {
        Path hdfsPath = new Path(path);
        return isFileExists(hdfsPath, conf);
    }

    public static boolean isFileExists(Path path, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        return fs.exists(path);
    }
    
    public static FSDataOutputStream createFile(Path file, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        return fs.create(file);
    }
    
    public static FSDataInputStream open(Path path, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        return fs.open(path);
    }
    
    private static SimpleDateFormat FORMAT = new SimpleDateFormat("MM-dd HH:mm");
    public static void log(Object context, String msg) {
        msg = String.format("[%s %s] : %s", context.getClass().getSimpleName(), FORMAT.format(new Date()), msg);
        System.out.println(msg);
    }
    
    public static SequenceFileDirIterator<IntWritable, VectorWritable> openVectorIterator
        (Path path, Configuration conf) throws IOException {
        return  new SequenceFileDirIterator<IntWritable, VectorWritable>(path,
                PathType.LIST, new PathFilter() {
                    
                    @Override
                    public boolean accept(Path path) {
                        return !path.getName().startsWith("_");
                    }
                }, null, true, conf);
    }
}
