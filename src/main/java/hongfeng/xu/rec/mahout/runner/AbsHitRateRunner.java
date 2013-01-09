/**
 * 2013-1-8
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.runner;

import hongfeng.xu.rec.mahout.eval.AbsHitRateEvaluator;
import hongfeng.xu.rec.mahout.util.L;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;

/**
 * @author xuhongfeng
 *
 */
public abstract class AbsHitRateRunner implements Runner {
    protected final AbsHitRateEvaluator evaluator;
    protected final Recommender recommender;
    protected final DataModel testDataModel;

    public AbsHitRateRunner(AbsHitRateEvaluator evaluator,
            Recommender recommender, DataModel testDataModel) {
        super();
        this.evaluator = evaluator;
        this.recommender = recommender;
        this.testDataModel = testDataModel;
    }

    @Override
    public final void exec() {
        for (int N=10; N<=100; N+=10) {
            try {
                double rate = evaluator.evaluate(recommender, testDataModel, N);
                L.i(this, "N = " + N);
                reportHitRate(rate * 100);
            } catch (TasteException e) {
                L.e(this, e);
                return;
            }
        }
    }
    
    protected abstract void reportHitRate(double rate);
}
