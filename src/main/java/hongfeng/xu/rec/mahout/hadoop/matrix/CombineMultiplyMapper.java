/**
 * 2013-3-18
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.matrix;

import hongfeng.xu.rec.mahout.hadoop.misc.IntDoubleWritable;
import hongfeng.xu.rec.mahout.hadoop.misc.IntIntWritable;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author xuhongfeng
 *
 */
public class CombineMultiplyMapper extends Mapper<IntIntWritable, DoubleWritable,
    IntWritable, IntDoubleWritable> {
    private IntWritable intWritable = new IntWritable();
    private IntDoubleWritable intDoubleWritable = new IntDoubleWritable();

    public CombineMultiplyMapper() {
        super();
    }

    @Override
    protected void map(IntIntWritable key, DoubleWritable value, Context context)
            throws IOException, InterruptedException {
        int row = key.getId1();
        int column = key.getId2();
        
        intWritable.set(row);
        intDoubleWritable.setId(column);
        intDoubleWritable.setValue(value.get());
        
        context.write(intWritable, intDoubleWritable);
    }
}
