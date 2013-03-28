/**
 * 2013-3-27
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.neighbor;

import hongfeng.xu.rec.mahout.hadoop.misc.IntIntWritable;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author xuhongfeng
 *
 */
public final class ParserReducer extends Reducer<IntWritable, IntIntWritable, IntIntWritable, IntWritable> {
    private IntIntWritable keyWritable = new IntIntWritable();
    private IntWritable valueWritable = new IntWritable();

    public ParserReducer() {
        super();
    }
    
    @Override
    protected void reduce(IntWritable key, Iterable<IntIntWritable> values, Context context)
            throws IOException, InterruptedException {
        Iterator<IntIntWritable> iterator = values.iterator();
        while (iterator.hasNext()) {
            IntIntWritable v = iterator.next();
            keyWritable.setId1(key.get());
            keyWritable.setId2(v.getId1());
            valueWritable.set(v.getId2());
            context.write(keyWritable, valueWritable);
        }
    }
}