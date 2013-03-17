/**
 * 2013-3-16
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.misc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * @author xuhongfeng
 *
 */
public class LongDoubleWritable implements WritableComparable<LongDoubleWritable>, Cloneable {
    private long id;
    private double value;

    public LongDoubleWritable() {
        super();
    }

    public LongDoubleWritable(long id, double value) {
        super();
        this.id = id;
        this.value = value;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readLong();
        value = in.readDouble();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(id);
        out.writeDouble(value);
    }
    
    @Override
    public int compareTo(LongDoubleWritable o) {
        if (id < o.id) {
            return -1;
        }
        if (id > o.id) {
            return 1;
        }
        if (value < o.value) {
            return -1;
        }
        if (value > o.value) {
            return 1;
        }
        return 0;
    }

    @Override
    protected LongDoubleWritable clone() throws CloneNotSupportedException {
        return new LongDoubleWritable(id, value);
    }

    @Override
    public String toString() {
        return "LongDoubleWritable [id=" + id + ", value=" + value + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (id ^ (id >>> 32));
        long temp;
        temp = Double.doubleToLongBits(value);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        LongDoubleWritable other = (LongDoubleWritable) obj;
        if (id != other.id)
            return false;
        if (Double.doubleToLongBits(value) != Double
                .doubleToLongBits(other.value))
            return false;
        return true;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }
    
}
