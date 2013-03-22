/**
 * 2013-3-17
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.misc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * @author xuhongfeng
 *
 */
public class IntVectorWritable extends VarIntWritable {
    private Vector vector;
    
    public IntVectorWritable() {
        super();
    }
    
    public void setVector(Vector vector) {
        this.vector = vector;
    }
    
    public Vector getVector() {
        return this.vector;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        VectorWritable vectorWritable = new VectorWritable();
        vectorWritable.readFields(in);
        vector = vectorWritable.get();
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        VectorWritable vectorWritable = new VectorWritable(vector);
        vectorWritable.write(out);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((vector == null) ? 0 : vector.hashCode());
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
        IntVectorWritable other = (IntVectorWritable) obj;
        if (vector == null) {
            if (other.vector != null)
                return false;
        } else if (!vector.equals(other.vector))
            return false;
        return true;
    }
    
    public int getInt() {
        return get();
    }
    
    public void setInt(int value) {
        set(value);
    }

    @Override
    public IntVectorWritable clone() {
        IntVectorWritable me = new IntVectorWritable();
        me.setInt(this.getInt());
        me.setVector(this.getVector());
        return me;
    }
}
