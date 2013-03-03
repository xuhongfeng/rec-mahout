/**
 * 2013-3-2
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.structure;

import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * @author xuhongfeng
 */
public class FixedSizePriorityQueue<E> extends PriorityQueue<E> {
    private static final long serialVersionUID = -9093055402214651780L;

    private final int size;

    private final Comparator<E> comparator;

    public FixedSizePriorityQueue(int size, Comparator<E> comparator) {
        super(size, comparator);
        this.size = size;
        this.comparator = comparator;
    }

    @Override
    public boolean add(E e) {
        if (size() == size) {
            if (comparator.compare(peek(), e) < 0) {
                poll();
                return super.add(e);
            } else {
                return false;
            }
        } else {
            return super.add(e);
        }
    }
}
