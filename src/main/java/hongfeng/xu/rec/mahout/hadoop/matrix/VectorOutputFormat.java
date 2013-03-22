/**
 * 2013-3-17
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.matrix;

import hongfeng.xu.rec.mahout.hadoop.MultipleSequenceOutputFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.mahout.math.VectorWritable;

/**
 * @author xuhongfeng
 *
 */
public class VectorOutputFormat extends MultipleSequenceOutputFormat<IntWritable, VectorWritable> {
    
    @Override
    protected String getFile(IntWritable key) {
        return String.valueOf(key.get()/1000);
    }
}
