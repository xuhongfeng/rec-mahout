/**
 * 2013-1-11
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.runner;

import hongfeng.xu.rec.mahout.eval.TopNEvaluator;
import hongfeng.xu.rec.mahout.util.L;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;

/**
 * @author xuhongfeng
 *
 */
public class AbsTopNRunner<T extends TopNEvaluator> extends AbsRateRunner {
    public static final int MAX_N = 100;
    public static final int MIN_N = 10;
    public static final int STEP = 10;
    
    protected final T evaluator;

    protected AbsTopNRunner(T evaluator, Recommender recommender
            , DataModel totalDataModel, DataModel testDataModel, String rateName) {
        super(recommender, totalDataModel, testDataModel, rateName);
        this.evaluator = evaluator;
    }

    @Override
    final public void exec() {
        for (int N=MIN_N; N<=MAX_N; N+=STEP) {
            try {
                double rate = evaluator.evaluate(recommender, totalDataModel, testDataModel, N);
                L.i(this, "N = " + N);
                reportRate(rate);
            } catch (TasteException e) {
                L.e(this, e);
                return;
            }
        }
        
    }
}
