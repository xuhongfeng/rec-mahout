/**
 * 2013-1-9
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.runner.movielens;

import hongfeng.xu.rec.mahout.eval.RecallRateEvaluator;
import hongfeng.xu.rec.mahout.runner.AbsRecallRateRunner;

import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;

/**
 * @author xuhongfeng
 *
 */
public class RecallRateRunner extends AbsRecallRateRunner {

    public RecallRateRunner(RecallRateEvaluator evaluator,
            Recommender recommender, DataModel testDataModel) {
        super(evaluator, recommender, testDataModel);
    }

}
