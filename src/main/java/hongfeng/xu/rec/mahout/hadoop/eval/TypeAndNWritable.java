/**
 * 2013-3-12
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.eval;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.mahout.math.VarIntWritable;

/**
 * @author xuhongfeng
 *
 */
public class TypeAndNWritable extends VarIntWritable {
    public static final int TYPE_COVERAGE = 1;
    public static final int TYPE_POPULARITY = TYPE_COVERAGE + 1;
    public static final int TYPE_PRECISION = TYPE_POPULARITY + 1;
    public static final int TYPE_RECALL = TYPE_PRECISION + 1;
    private int n;

    public TypeAndNWritable() {
        super();
    }

    public TypeAndNWritable(int type, int n) {
        super(type);
        this.n = n;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(n);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        n = in.readInt();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + n;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        TypeAndNWritable other = (TypeAndNWritable) obj;
        if (n != other.n)
            return false;
        return true;
    }
    
    @Override
    public TypeAndNWritable clone() {
        return new TypeAndNWritable(get(), n);
    }

    @Override
    public String toString() {
        return "TypeAndNWritable [n=" + n + ", get()=" + get() + "]";
    }

    public int getN() {
        return n;
    }
    
    public int getType() {
        return get();
    }
    
    @Override
    public int compareTo(VarIntWritable o) {
        TypeAndNWritable other = (TypeAndNWritable) o;
        if (n < other.getN()) {
            return -1;
        }
        if (n > other.getN()) {
            return 1;
        }
        return super.compareTo(o);
    }
}
