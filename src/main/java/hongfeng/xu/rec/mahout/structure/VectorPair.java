/**
 * 2013-3-18
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.structure;


import org.apache.mahout.math.VectorWritable;

/**
 * @author xuhongfeng
 *
 */
public class VectorPair extends PairWritable<VectorWritable, VectorWritable> {
    
    public VectorPair() {
        super();
    }

    @Override
    protected VectorWritable newFirst() {
        return new VectorWritable();
    }

    @Override
    protected VectorWritable newSecond() {
        return new VectorWritable();
    }

}
