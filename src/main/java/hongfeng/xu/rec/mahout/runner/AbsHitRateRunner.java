/**
 * 2013-1-8
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.runner;

import hongfeng.xu.rec.mahout.eval.AbsHitRateEvaluator;
import hongfeng.xu.rec.mahout.eval.ItemBasedRecommenderBuilder;
import hongfeng.xu.rec.mahout.util.L;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;

/**
 * @author xuhongfeng
 *
 */
public abstract class AbsHitRateRunner extends AbsFileDataModelRunner {

    @Override
    protected final void innerExec(FileDataModel dataModel) {
        AbsHitRateEvaluator evaluator = createEvaluator();
        ItemBasedRecommenderBuilder recommenderBuilder = new ItemBasedRecommenderBuilder();
        for (int N=10; N<=100; N+=10) {
            try {
                double rate = evaluator.evaluate(recommenderBuilder, dataModel, 0.8, 1, N);
                L.i(this, "N = " + N);
                reportHitRate(rate * 100);
            } catch (TasteException e) {
                L.e(this, e);
                return;
            }
        }
    }
    
    protected abstract AbsHitRateEvaluator createEvaluator();
    protected abstract void reportHitRate(double rate);
}
