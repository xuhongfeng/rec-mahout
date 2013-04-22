/**
 * 2013-3-10
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop;

import hongfeng.xu.rec.mahout.config.DataSetConfig;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

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
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.function.DoubleDoubleFunction;
import org.apache.mahout.math.function.DoubleFunction;

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
    
    public static int sub(Vector vector1, Vector vector2) {
        if (vector1.size() != vector2.size()) {
            throw new RuntimeException();
        }
        Set<Integer> set = new HashSet<Integer>();
        Iterator<Element> it = vector2.iterateNonZero();
        while (it.hasNext()) {
            set.add(it.next().index());
        }
        int count = 0;
        while (it.hasNext()) {
            if (!set.contains(it.next().index())) {
                count ++;
            }
        }
        return count;
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
            if (arg1 != 0 && arg2 != 0) {
                return 1;
            }
            return 0;
        }
    };
    
    public static double cosineSimilarity(Vector vector1, Vector vector2) {
        double dot = vector1.dot(vector2);
        if (dot == 0.0) {
            return  0.0;
        }
        if (DataSetConfig.ONE_ZERO) {
            double len = Math.sqrt(vector1.zSum()*vector2.zSum());
            double r = dot/len;
            if (r > 1.0) {
                throw new RuntimeException("r="+r+", len="+len + ",dot="+dot);
            }
            return r;
        } else {
            throw new RuntimeException();
        }
    }
    
    public static int numNonZero(Vector vector) {
        return (int) vector.aggregate(aggregatorNumNonZero, mapNumNonZero);
    }
    private static DoubleFunction mapNumNonZero = new DoubleFunction() {
        public double apply(double arg1) {
            if (arg1 == 0.0) {
                return 0.0;
            }
            return 1.0;
        }
    };
    private static DoubleDoubleFunction aggregatorNumNonZero = new DoubleDoubleFunction() {
        @Override
        public double apply(double arg1, double arg2) {
            return arg1 + arg2;
        }
    };
}
