/**
 * 2013-1-8
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.runner;

import hongfeng.xu.rec.mahout.eval.RecallRateEvaluator;

import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;


/**
 * @author xuhongfeng
 *
 */
public class RecallRateRunner extends AbsHitRateRunner<RecallRateEvaluator> {
    private static final String RATE_NAME = "recall";

    public RecallRateRunner(RecallRateEvaluator evaluator,
            Recommender recommender, DataModel totalDataModel,
            DataModel testDataModel) {
        super(evaluator, recommender, totalDataModel, testDataModel, RATE_NAME);
    }
}
