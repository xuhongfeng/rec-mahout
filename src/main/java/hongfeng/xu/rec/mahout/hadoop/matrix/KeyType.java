/**
 * 2013-3-17
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.matrix;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * @author xuhongfeng
 *
 */
public class KeyType implements WritableComparable<KeyType>, Cloneable {
    public static final int TYPE_ROW = 0;
    public static final int TYPE_COLUMN = TYPE_ROW + 1;
    
    private int type;
    private int index;

    public KeyType() {
        super();
    }

    public KeyType(int type, int index) {
        super();
        this.type = type;
        this.index = index;
    }
    
    

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        type  = in.readInt();
        index = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(type);
        out.writeInt(index);
    }

    @Override
    public int compareTo(KeyType o) {
        if (type < o.type) {
            return -1;
        }
        if (type > o.type) {
            return 1;
        }
        if (index < o.index) {
            return -1;
        }
        if (index > o.index) {
            return 1;
        }
        return 0;
    }

    @Override
    public String toString() {
        return "KeyType [type=" + type + ", index=" + index + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + index;
        result = prime * result + type;
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
        KeyType other = (KeyType) obj;
        if (index != other.index)
            return false;
        if (type != other.type)
            return false;
        return true;
    }

    protected KeyType clone() throws CloneNotSupportedException {
        return new KeyType(type, index);
    };
}
