/**
 * 2013-3-10
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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
}
