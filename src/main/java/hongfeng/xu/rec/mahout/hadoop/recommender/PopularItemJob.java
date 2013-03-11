/**
 * 2013-3-10
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.recommender;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.VectorWritable;

/**
 * @author xuhongfeng
 *
 */
public class PopularItemJob extends AbstractJob {

    @Override
    public int run(String[] args) throws Exception {
        addInputOption();
        addOutputOption();
        
        Map<String,List<String>> parsedArgs = parseArguments(args);
        if (parsedArgs == null) {
          return -1;
        }
        
        Job job = prepareJob(getInputPath(), getOutputPath(), SequenceFileInputFormat.class,
                MyMapper.class, IntWritable.class, MidType.class, MyReducer.class,
                IntWritable.class, DoubleWritable.class, SequenceFileOutputFormat.class);
        job.setJobName(getClass().getSimpleName());
        if (!job.waitForCompletion(true)) {
            return -1;
        }
        return 0;
    }
    
    public static class MyMapper extends Mapper<IntWritable, VectorWritable, IntWritable, MidType> {
        private IntWritable ONE = new IntWritable(1);
        public MyMapper() {
            super();
        }

        @Override
        protected void map(IntWritable key, VectorWritable value, Context context)
                throws IOException, InterruptedException {
            context.write(ONE, new MidType(key.get(), value.get().zSum()));
        }
        
    }
    
    public static class MyReducer extends Reducer<IntWritable, MidType, IntWritable, DoubleWritable> {

        public MyReducer() {
            super();
        }

        @Override
        protected void reduce(IntWritable key, Iterable<MidType> values,
                Context context) throws IOException, InterruptedException {
            List<MidType> list = new ArrayList<MidType>();
            for(MidType value:values) {
                list.add(value);
            }
            Collections.sort(list, new Comparator<MidType>() {

                @Override
                public int compare(MidType o1, MidType o2) {
                    if (o1.getValue() > o2.getValue()) {
                        return -1;
                    } else if (o1.getValue() < o2.getValue()) {
                        return 1;
                    }
                    return 0;
                }
            });
            
            IntWritable intWritable = new IntWritable();
            DoubleWritable doubleWritable = new DoubleWritable();
            for (MidType value:list) {
                intWritable.set(value.getIndex());
                doubleWritable.set(value.getValue());
                context.write(intWritable, doubleWritable);
            }
        }
    }
    
    public static class MidType implements WritableComparable<MidType>, Cloneable {
        private int index;
        private double value;
        
        public MidType() {
            super();
        }

        public MidType(int index, double value) {
            super();
            this.index = index;
            this.value = value;
        }
        
        public int getIndex() {
            return index;
        }

        public void setIndex(int index) {
            this.index = index;
        }

        public double getValue() {
            return value;
        }

        public void setValue(double value) {
            this.value = value;
        }

        @Override
        public void readFields(DataInput input) throws IOException {
            index = input.readInt();
            value = input.readDouble();
        }

        @Override
        public void write(DataOutput output) throws IOException {
            output.writeInt(index);
            output.writeDouble(value);
        }

        @Override
        public int compareTo(MidType o) {
            if (index < o.index) {
                return -1;
            } else if (index < o.index) {
                return 1;
            }
            return 0;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + index;
            long temp;
            temp = Double.doubleToLongBits(value);
            result = prime * result + (int) (temp ^ (temp >>> 32));
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            return (obj instanceof MidType)
                    && index == ((MidType)obj).index
                    && value == ((MidType)obj).value;
        }
        
        @Override
        protected Object clone() throws CloneNotSupportedException {
            return new MidType(index, value);
        }
    }
}
