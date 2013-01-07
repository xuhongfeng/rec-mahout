/**
 * 2013-1-6
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.eval;

/**
 * @author xuhongfeng
 *
 */
public class RecallRateEvaluator extends AbsHitRateEvaluator {

    @Override
    protected int calculateAll(int oldAll, int recomendSize, int userItemSize) {
        return oldAll + userItemSize;
    }

}
