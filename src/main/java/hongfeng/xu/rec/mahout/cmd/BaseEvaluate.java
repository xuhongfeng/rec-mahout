/**
 * 2013-2-23
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.cmd;

import hongfeng.xu.rec.mahout.chart.ChartDrawer;
import hongfeng.xu.rec.mahout.eval.CoverageEvaluator;
import hongfeng.xu.rec.mahout.eval.PopularityEvaluator;
import hongfeng.xu.rec.mahout.eval.PrecisionRateEvaluator;
import hongfeng.xu.rec.mahout.eval.RecallRateEvaluator;
import hongfeng.xu.rec.mahout.recommender.PopularRecommender;
import hongfeng.xu.rec.mahout.recommender.PreCachingRecommender;
import hongfeng.xu.rec.mahout.runner.AbsHitRateRunner;
import hongfeng.xu.rec.mahout.runner.AbsTopNRunner;
import hongfeng.xu.rec.mahout.runner.AbsTopNRunner.Result;
import hongfeng.xu.rec.mahout.runner.CoverageRateRunner;
import hongfeng.xu.rec.mahout.runner.PopularityRunner;
import hongfeng.xu.rec.mahout.runner.PrecisionRateRunner;
import hongfeng.xu.rec.mahout.runner.RecallRateRunner;
import hongfeng.xu.rec.mahout.util.DataModelUtils;
import hongfeng.xu.rec.mahout.util.L;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.recommender.RandomRecommender;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.common.Pair;

/**
 * @author xuhongfeng
 *
 */
public abstract class BaseEvaluate {
    private static final double DEFAULT_DATA_PERCENTAGE = 1;
    
    protected DataModel totalDataModel;
    protected DataModel trainingDataModel;
    protected DataModel testDataModel;
    
    protected RecallRateEvaluator recallRateEvaluator;
    protected PrecisionRateEvaluator precisionRateEvaluator;
    protected CoverageEvaluator coverageEvaluator;
    protected PopularityEvaluator popularityEvaluator;
    
    protected Map<String, Result> recallResult;
    protected Map<String, Result> precisionResult;
    protected Map<String, Result> coverageResult;
    protected Map<String, Result> popularityResult;
    
    protected double getDataPercentage() {
        return DEFAULT_DATA_PERCENTAGE;
    }
    
    protected void initDataModel() {
        L.i(this, "create data model");
        try {
            totalDataModel = getDataModel();
        } catch (IOException e) {
            L.e(this, e);
            return;
        }
        L.i(this, "split data model");
        try {
            Pair<DataModel, DataModel> models =
                    DataModelUtils.split(totalDataModel, getDataPercentage(), 0.8);
            trainingDataModel = models.getFirst();
            testDataModel = models.getSecond();
        } catch (TasteException e) {
            L.e(this, e);
            return;
        }
    }
    
    public void init() {
        initDataModel();
        initRecommender();
        initEvaluator();
        
        recallResult = new HashMap<String, Result>();
        precisionResult = new HashMap<String, Result>();
        coverageResult = new HashMap<String, Result>();
        popularityResult = new HashMap<String, Result>();
    }
    
    public void exec() {
    }
    
    protected void evaluatePopularity(Recommender recommender, String resultName) {
        PopularityRunner runner = new PopularityRunner(popularityEvaluator, recommender,
                totalDataModel, testDataModel);
        execRunner(runner, popularityResult, resultName);
    }
    
    protected void evaluateCoverageRate(Recommender recommender, String resultName) {
        CoverageRateRunner runner = new CoverageRateRunner(coverageEvaluator, recommender,
                totalDataModel, testDataModel);
        execRunner(runner, coverageResult, resultName);
    }
    
    protected void evaluatePrecisionRate(Recommender recommender, String resultName) {
        PrecisionRateRunner runner = new PrecisionRateRunner(precisionRateEvaluator, recommender,
                totalDataModel, testDataModel);
        execRunner(runner, precisionResult, resultName);
    }
    
    protected void evaluateRecallRate(Recommender recommender, String resultName) {
        RecallRateRunner runner = new RecallRateRunner(recallRateEvaluator, recommender,
                totalDataModel, testDataModel);
        execRunner(runner, recallResult, resultName);
    }
    
    private void execRunner(AbsTopNRunner<?> runner, Map<String, Result> resultMap
            , String resultName) {
        runner.exec();
        Result result = runner.getResultMap();
        resultMap.put(resultName, result);
    }
    
    protected void drawChart(Map<String, Result> resultMap, String chartTitle, String yLabel, String imageFile
            , boolean withPercentage) {
        try {
            new ChartDrawer(chartTitle, yLabel, imageFile, resultMap, withPercentage).draw();
        } catch (IOException e) {
            L.e(this, e);
            return;
        }
    }
    
    protected void initRecommender() {}
    
    protected void initEvaluator() {
        recallRateEvaluator = new RecallRateEvaluator();
        precisionRateEvaluator = new PrecisionRateEvaluator();
        coverageEvaluator = new CoverageEvaluator();
        popularityEvaluator = new PopularityEvaluator();
    }
    
    protected abstract DataModel getDataModel() throws IOException;
    
    private Recommender wrapPreCacheRecommender(Recommender originRecommender) throws TasteException {
        return new PreCachingRecommender(originRecommender, AbsHitRateRunner.MAX_N);
    }
    
    protected Recommender createPopularRecommender() throws TasteException {
        return wrapPreCacheRecommender(new PopularRecommender(trainingDataModel));
    }
    
    protected Recommender createRandomRecommender() throws TasteException {
        return wrapPreCacheRecommender(new RandomRecommender(trainingDataModel));
    }
}
