/**
 * 2013-3-17
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.matrix;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.VectorWritable;

/**
 * @author xuhongfeng
 *
 */
public class MatrixMapper extends Mapper<IntWritable, VectorWritable,
        IntWritable, VectorWritable> {
    
    public MatrixMapper() {
        super();
    }
    
    @Override
    protected void map(IntWritable key, VectorWritable value, Context context)
            throws IOException, InterruptedException {
        context.write(key, value);
    }
}
