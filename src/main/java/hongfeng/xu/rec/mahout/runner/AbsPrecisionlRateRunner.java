/**
 * 2013-1-8
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.runner;

import hongfeng.xu.rec.mahout.eval.AbsHitRateEvaluator;
import hongfeng.xu.rec.mahout.eval.RecallRateEvaluator;
import hongfeng.xu.rec.mahout.util.L;


/**
 * @author xuhongfeng
 *
 */
public abstract class AbsPrecisionlRateRunner extends AbsHitRateRunner {

    @Override
    protected AbsHitRateEvaluator createEvaluator() {
        return new RecallRateEvaluator();
    }

    @Override
    protected void reportHitRate(double rate) {
        L.i("Main", "precision rate = %.2f%%", rate);
    }
}
