/**
 * 2013-3-10
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.parser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * @author xuhongfeng
 *
 */
public final class KeyType implements WritableComparable<KeyType>, Cloneable {
    public static final int TYPE_USER_TAG = 1;
    public static final int TYPE_USER_ITEM = 2;
    public static final int TYPE_ITEM_TAG = 3;
    
    private int type;
    private long id1;
    private long id2;

    public KeyType() {
        super();
    }

    public KeyType(int type, long id1, long id2) {
        super();
        this.type = type;
        this.id1 = id1;
        this.id2 = id2;
    }
    
    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public long getId1() {
        return id1;
    }

    public void setId1(long id1) {
        this.id1 = id1;
    }

    public long getId2() {
        return id2;
    }

    public void setId2(long id2) {
        this.id2 = id2;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (id1 ^ (id1 >>> 32));
        result = prime * result + (int) (id2 ^ (id2 >>> 32));
        result = prime * result + type;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof KeyType) && compareTo((KeyType) obj)==0;
    }

    @Override
    public String toString() {
        return "KeyType [type=" + type + ", id1=" + id1 + ", id2=" + id2
                + "]";
    }
    
    @Override
    protected Object clone() throws CloneNotSupportedException {
        return new KeyType(type, id1, id2);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        type = input.readInt();
        id1 = input.readLong();
        id2 = input.readLong();
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(type);
        output.writeLong(id1);
        output.writeLong(id2);
    }

    @Override
    public int compareTo(KeyType o) {
        if (type < o.type) {
            return -1;
        }
        if (type > o.type) {
            return 1;
        }
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
    }