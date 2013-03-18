/**
 * 2013-3-18
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.matrix;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterator;
import org.apache.mahout.math.ConstantVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * @author xuhongfeng
 *
 */
public class VectorCache {
    private Vector[] vectors;
    
    private Vector EMPTY_VECTOR;
    
    private VectorCache () {}
    
    private void init (int size) {
        vectors = new Vector[size];
        EMPTY_VECTOR = new ConstantVector(0.0, size);
    }
    
    private void add(int index, Vector vector) {
        vectors[index] = vector;
    }
    
    public Vector get(int index) {
        return vectors[index];
    }
    
    public static VectorCache create(int size, Path path, Configuration conf) throws IOException {
        VectorCache cache = new VectorCache();
        cache.init(size);
        SequenceFileIterator<IntWritable, VectorWritable> iterator = new 
                SequenceFileIterator<IntWritable, VectorWritable>(path, true, conf);
        try {
            while (iterator.hasNext()) {
                Pair<IntWritable, VectorWritable> pair = iterator.next();
                cache.add(pair.getFirst().get(), pair.getSecond().get());
            }
            for (int i=0; i<cache.vectors.length; i++) {
                if (cache.vectors[i] == null) {
                    cache.vectors[i] = cache.EMPTY_VECTOR;
                }
            }
            return cache;
        } finally {
            iterator.close();
        }
    }
}
