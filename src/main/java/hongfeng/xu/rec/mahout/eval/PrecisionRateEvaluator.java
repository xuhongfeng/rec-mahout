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
public class PrecisionRateEvaluator extends AbsHitRateEvaluator {

    @Override
    protected int calculateAll(int oldAll, int recomendSize, int userItemSize) {
        return oldAll + recomendSize;
    }

}
