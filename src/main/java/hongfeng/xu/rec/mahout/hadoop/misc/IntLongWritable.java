/**
 * 2013-3-17
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
public class IntLongWritable implements WritableComparable<IntLongWritable>, Cloneable {
    private int index;
    private long id;

    public IntLongWritable(int index, long id) {
        super();
        this.index = index;
        this.id = id;
    }

    public IntLongWritable() {
        super();
    }
    
    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        index = in.readInt();
        id = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(index);
        out.writeLong(id);
    }

    @Override
    public int compareTo(IntLongWritable o) {
        if (index < o.index) {
            return -1;
        }
        if (index > o.index) {
            return 1;
        }
        if (id < o.id) {
            return -1;
        }
        if (id > o.id) {
            return 1;
        }
        return 0;
    }
    

    @Override
    public String toString() {
        return "IntLongWritable [index=" + index + ", id=" + id + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (id ^ (id >>> 32));
        result = prime * result + index;
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
        IntLongWritable other = (IntLongWritable) obj;
        if (id != other.id)
            return false;
        if (index != other.index)
            return false;
        return true;
    }

    @Override
    protected IntLongWritable clone() throws CloneNotSupportedException {
        return new IntLongWritable(index, id);
    }
}
