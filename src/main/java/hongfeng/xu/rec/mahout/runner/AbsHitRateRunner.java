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
    public static final int MAX_N = 100;
    public static final int MIN_N = 10;
    public static final int STEP = 10;
    
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
        for (int N=MIN_N; N<=MAX_N; N+=STEP) {
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
