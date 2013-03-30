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
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.function.DoubleDoubleFunction;

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
    
    public static int intersect(Vector vector1, Vector vector2) {
        return (int) vector1.aggregate(vector2, aggregator, combiner);
    }
    
    private static DoubleDoubleFunction aggregator = new DoubleDoubleFunction() {
        @Override
        public double apply(double arg1, double arg2) {
            return arg1 + arg2;
        }
    };
    
    private static DoubleDoubleFunction combiner = new DoubleDoubleFunction() {
        @Override
        public double apply(double arg1, double arg2) {
            if (arg1>0 && arg2>0) {
                return 1;
            }
            return 0;
        }
    };
    
    public static double cosinSimilarity(Vector vector1, Vector vector2) {
        return vector1.dot(vector2)/(Math.sqrt(vector1.getLengthSquared()) * Math.sqrt(vector2.getLengthSquared()));
    }
    
    public static double pearsonSimilarity(Vector vector1, Vector vector2) {
        double size = vector1.size();
        double mean1 = vector1.zSum()/size;
        double mean2 = vector2.zSum()/size;
        Vector v1 = vector1.plus(0-mean1);
        Vector v2 = vector2.plus(0-mean2);
        return cosinSimilarity(v1, v2);
    }
}
