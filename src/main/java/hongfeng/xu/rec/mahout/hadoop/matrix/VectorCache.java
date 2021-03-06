/**
 * 2013-3-18
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.matrix;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterator;
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
    
    private void init (int vectorCount, int vectorSize) {
        vectors = new Vector[vectorCount];
        EMPTY_VECTOR = new ConstantVector(0.0, vectorSize);
    }
    
    private void add(int index, Vector vector) {
        vectors[index] = vector;
    }
    
    public Vector get(int index) {
        return vectors[index];
    }
    
    public int size() {
        return vectors.length;
    }
    
    public static VectorCache create(int vectorCount, int vectorSize, Path path, Configuration conf) throws IOException {
        boolean checked = false;;
        VectorCache cache = new VectorCache();
        cache.init(vectorCount, vectorSize);
        SequenceFileDirIterator<IntWritable, VectorWritable> iterator = new
                SequenceFileDirIterator<IntWritable, VectorWritable>(path,
                PathType.LIST, new PathFilter() {
                    @Override
                    public boolean accept(Path path) {
                        return !path.getName().startsWith("_");
                    }
                }, null, true, conf);
        try {
            int count = 0;
            while (iterator.hasNext()) {
                Pair<IntWritable, VectorWritable> pair = iterator.next();
                Vector vector = pair.getSecond().get();
                
                if (!checked && vector.size()!=vectorSize) {
                    throw new RuntimeException("vector.size="+vector.size() + ", vectorSize="+vectorSize
                            + ", path = " + path);
                }
                checked=true;
                
                cache.add(pair.getFirst().get(), vector);
                count ++;
            }
            if (count != vectorCount) {
                throw new RuntimeException("count = " + count + ", vectorCount=" + vectorCount
                         + ", path = " + path);
            }
//            for (int i=0; i<cache.vectors.length; i++) {
//                if (cache.vectors[i] == null) {
//                    cache.vectors[i] = cache.EMPTY_VECTOR;
//                }
//            }
            return cache;
        } finally {
            iterator.close();
        }
    }
}
