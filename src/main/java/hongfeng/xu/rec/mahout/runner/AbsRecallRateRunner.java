/**
 * 2013-1-8
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.runner;

import hongfeng.xu.rec.mahout.eval.AbsHitRateEvaluator;
import hongfeng.xu.rec.mahout.util.L;

import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;


/**
 * @author xuhongfeng
 *
 */
public abstract class AbsRecallRateRunner extends AbsHitRateRunner {

    public AbsRecallRateRunner(AbsHitRateEvaluator evaluator,
            Recommender recommender, DataModel testDataModel) {
        super(evaluator, recommender, testDataModel);
    }

    @Override
    protected void reportHitRate(double rate) {
        L.i("Main", "recall rate = %.2f%%", rate);
    }
}
