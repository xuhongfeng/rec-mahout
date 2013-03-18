/**
 * 2013-3-17
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.misc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * @author xuhongfeng
 *
 */
public class IntVectorWritable implements Writable {
    private IntWritable intWritable = new IntWritable();
    private VectorWritable vectorWritable = new VectorWritable();
    
    public IntVectorWritable() {
        super();
    }
    
    public void setInt(int type) {
        intWritable.set(type);
    }
    
    public void setVector(Vector vector) {
        vectorWritable.set(vector);
    }
    
    public IntWritable getIntWritable() {
        return intWritable;
    }

    public void setIntWritable(IntWritable intWritable) {
        this.intWritable = intWritable;
    }

    public VectorWritable getVectorWritable() {
        return vectorWritable;
    }

    public void setVectorWritable(VectorWritable vectorWritable) {
        this.vectorWritable = vectorWritable;
    }



    public IntVectorWritable(IntWritable intWritable,
            VectorWritable vectorWritable) {
        super();
        this.intWritable = intWritable;
        this.vectorWritable = vectorWritable;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        intWritable.readFields(in);
        vectorWritable.readFields(in);
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        intWritable.write(out);
        vectorWritable.write(out);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((intWritable == null) ? 0 : intWritable.hashCode());
        result = prime * result
                + ((vectorWritable == null) ? 0 : vectorWritable.hashCode());
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
        IntVectorWritable other = (IntVectorWritable) obj;
        if (intWritable == null) {
            if (other.intWritable != null)
                return false;
        } else if (!intWritable.equals(other.intWritable))
            return false;
        if (vectorWritable == null) {
            if (other.vectorWritable != null)
                return false;
        } else if (!vectorWritable.equals(other.vectorWritable))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "IntVectorWritable [intWritable=" + intWritable
                + ", vectorWritable=" + vectorWritable + "]";
    }
    
}
