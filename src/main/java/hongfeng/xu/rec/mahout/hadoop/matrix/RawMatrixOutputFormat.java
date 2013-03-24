/**
 * 2013-3-22
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.matrix;

import hongfeng.xu.rec.mahout.hadoop.MultipleSequenceOutputFormat;
import hongfeng.xu.rec.mahout.hadoop.misc.IntIntWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;

/**
 * @author xuhongfeng
 *
 */
public class RawMatrixOutputFormat extends MultipleSequenceOutputFormat<IntIntWritable, DoubleWritable> {
    @Override
    protected String getFile(IntIntWritable key, Configuration conf) {
        int vectorCount = conf.getInt("vectorCount", -1);
        if (vectorCount == -1) {
            return String.valueOf(key.getId1()%10);
        } else {
            return String.valueOf(key.getId1()/(vectorCount/10));
        }
    }
}
