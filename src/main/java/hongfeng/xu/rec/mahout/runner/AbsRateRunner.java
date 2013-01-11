/**
 * 2013-1-11
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.runner;

import hongfeng.xu.rec.mahout.util.L;

import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;

/**
 * @author xuhongfeng
 *
 */
public abstract class AbsRateRunner implements Runner {
    private final String RATE_NAME;
    protected final Recommender recommender;
    protected final DataModel testDataModel;
    protected final DataModel totalDataModel;

    protected AbsRateRunner(Recommender recommender, DataModel totalDataModel 
            , DataModel testDataModel, String rateName) {
        this.RATE_NAME = rateName;
        this.recommender = recommender;
        this.testDataModel = testDataModel;
        this.totalDataModel = totalDataModel;
    }
    
    protected void reportRate (double rate) {
        L.i(this, RATE_NAME + " rate = %.2f%%", rate*100);
    }
}
