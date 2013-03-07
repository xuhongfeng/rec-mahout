/**
 * 2013-3-10
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.parser;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author xuhongfeng
 *
 */
public final class ParserReducer extends Reducer<KeyType, DoubleWritable, KeyType, DoubleWritable> {
    @Override
    protected void reduce(KeyType key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        double value = 0.0;
        for (DoubleWritable v:values) {
            value += v.get();
        }
        context.write(key, new DoubleWritable(value));
    }

    public ParserReducer() {
        super();
    }
    
}