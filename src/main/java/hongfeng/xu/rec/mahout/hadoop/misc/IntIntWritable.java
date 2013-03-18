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
public class IntIntWritable implements WritableComparable<IntIntWritable>, Cloneable {
    private int id1;
    private int id2;

    public IntIntWritable(int id1, int id2) {
        super();
        this.id1 = id1;
        this.id2 = id2;
    }

    public IntIntWritable() {
        super();
    }

    public int getId1() {
        return id1;
    }

    public void setId1(int id1) {
        this.id1 = id1;
    }

    public int getId2() {
        return id2;
    }

    public void setId2(int id2) {
        this.id2 = id2;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id1 = in.readInt();
        id2 = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id1);
        out.writeInt(id2);
    }

    @Override
    public int compareTo(IntIntWritable o) {
        if (id1 < o.id1) {
            return -1;
        }
        if (id1 > o.id1) {
            return 1;
        }
        if (id2 < o.id2) {
            return -1;
        }
        if (id2 > o.id2) {
            return 1;
        }
        return 0;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + id1;
        result = prime * result + id2;
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
        IntIntWritable other = (IntIntWritable) obj;
        if (id1 != other.id1)
            return false;
        if (id2 != other.id2)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "IntIntWritable [id1=" + id1 + ", id2=" + id2 + "]";
    }

    @Override
    protected IntIntWritable clone() throws CloneNotSupportedException {
        return new IntIntWritable(id1, id2);
    }
}
