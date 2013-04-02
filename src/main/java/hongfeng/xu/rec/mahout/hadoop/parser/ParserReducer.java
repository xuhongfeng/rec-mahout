/**
 * 2013-3-27
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.parser;

import hongfeng.xu.rec.mahout.hadoop.misc.IntDoubleWritable;
import hongfeng.xu.rec.mahout.hadoop.misc.IntIntWritable;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author xuhongfeng
 *
 */
public final class ParserReducer extends Reducer<IntWritable, IntDoubleWritable, IntIntWritable, DoubleWritable> {
    private IntIntWritable keyWritable = new IntIntWritable();
    private DoubleWritable valueWritable = new DoubleWritable();

    public ParserReducer() {
        super();
    }
    
    @Override
    protected void reduce(IntWritable key, Iterable<IntDoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        Iterator<IntDoubleWritable> iterator = values.iterator();
        while (iterator.hasNext()) {
            IntDoubleWritable v = iterator.next();
            keyWritable.setId1(key.get());
            keyWritable.setId2(v.getId());
            valueWritable.set(v.getValue());
            context.write(keyWritable, valueWritable);
        }
    }
}