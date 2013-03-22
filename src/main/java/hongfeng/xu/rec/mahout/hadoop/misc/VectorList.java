/**
 * 2013-3-21
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.misc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * @author xuhongfeng
 *
 */
public class VectorList implements Writable , Iterable<Vector> {
    private List<Vector> list = new ArrayList<Vector>();

    @Override
    public void write(DataOutput out) throws IOException {
        if (list != null) {
            out.write(list.size());
            VectorWritable writable = new VectorWritable();
            for (Vector vector:this.list) {
                writable.set(vector);
                writable.write(out);
            }
        } else {
            out.writeInt(0);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        list.clear();
        VectorWritable writable = new VectorWritable();
        for (int i=0; i<size; i++) {
            writable.readFields(in);
            list.add(writable.get());
        }
    }

    public List<Vector> getList() {
        return list;
    }

    public void setList(List<Vector> list) {
        this.list = list;
    }

    @Override
    public Iterator<Vector> iterator() {
        return list.iterator();
    }
    
    public int size() {
        return list.size();
    }
    
    public void clear() {
        list.clear();
    }
    
    public void add(Vector vector) {
        list.add(vector);
    }
}
