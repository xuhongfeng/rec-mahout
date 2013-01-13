/**
 * 2013-1-11
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.runner;

import hongfeng.xu.rec.mahout.eval.TopNEvaluator;
import hongfeng.xu.rec.mahout.util.L;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    protected int N;
    protected final Result resultMap = new Result();

    protected AbsTopNRunner(T evaluator, Recommender recommender
            , DataModel totalDataModel, DataModel testDataModel, String rateName) {
        super(recommender, totalDataModel, testDataModel, rateName);
        this.evaluator = evaluator;
    }

    @Override
    final public void exec() {
        resultMap.clear();
        for (N=MIN_N; N<=MAX_N; N+=STEP) {
            try {
                double rate = evaluator.evaluate(recommender, totalDataModel, testDataModel, N);
                L.i(this, "N = " + N);
                reportRate(rate);
                resultMap.put(N, rate);
            } catch (TasteException e) {
                L.e(this, e);
                return;
            }
        }
        
    }
    
    @Override
    protected void reportRate(double rate) {
        super.reportRate(rate);
    }
    
    public Result getResultMap() {
        return resultMap;
    }
    
    public static class Result {
        private final Map<Integer, Double> map;
        
        public Result() {
            map = new HashMap<Integer, Double>();
        }
        
        public void put(int N, double rate) {
            map.put(N, rate);
        }
        
        public void clear() {
            map.clear();
        }
        
        public List<Integer> listN() {
            Set<Integer> setN = map.keySet();
            List<Integer> listN = new ArrayList<Integer>();
            listN.addAll(setN);
            Collections.sort(listN);
            return listN;
        }
        
        public double getValue(int N) {
            return map.get(N);
        }
    }
}
