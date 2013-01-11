/**
 * 2013-1-8
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.runner;

import hongfeng.xu.rec.mahout.eval.AbsHitRateEvaluator;

import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;

/**
 * @author xuhongfeng
 *
 */
public abstract class AbsHitRateRunner<T extends AbsHitRateEvaluator>
        extends AbsTopNRunner<T> {

    protected AbsHitRateRunner(T evaluator, Recommender recommender,
            DataModel totalDataModel, DataModel testDataModel, String rateName) {
        super(evaluator, recommender, totalDataModel, testDataModel, rateName);
    }
}
