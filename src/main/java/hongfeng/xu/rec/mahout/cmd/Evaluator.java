/**
 * 2013-1-10
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.cmd;

import hongfeng.xu.rec.mahout.chart.ChartDrawer;
import hongfeng.xu.rec.mahout.eval.CoverageEvaluator;
import hongfeng.xu.rec.mahout.eval.PopularityEvaluator;
import hongfeng.xu.rec.mahout.eval.PrecisionRateEvaluator;
import hongfeng.xu.rec.mahout.eval.RecallRateEvaluator;
import hongfeng.xu.rec.mahout.model.MovielensModel;
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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.ItemAverageRecommender;
import org.apache.mahout.cf.taste.impl.recommender.RandomRecommender;
import org.apache.mahout.cf.taste.impl.recommender.slopeone.SlopeOneRecommender;
import org.apache.mahout.cf.taste.impl.similarity.CachingItemSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.CachingUserSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.common.Pair;


/**
 * @author xuhongfeng
 *
 */
public class Evaluator {
//    private static final String DATA_FILE = "data/movielens-1m/ratings.dat";
    private static final String DATA_FILE = "data/movielens-100k/u.data";
    private static final double DataPercentage = 1;
    
    private Recommender randomRecommender;
    private Recommender itemAverageRecommender;
    private Recommender itemBasedRecommender;
    private Recommender userBasedRecommender;
    private Recommender slopeOneRecommender;
    
    private DataModel totalDataModel;
    private DataModel trainingDataModel;
    private DataModel testDataModel;
    
    private RecallRateEvaluator recallRateEvaluator;
    private PrecisionRateEvaluator precisionRateEvaluator;
    private CoverageEvaluator coverageEvaluator;
    private PopularityEvaluator popularityEvaluator;
    
    private Map<String, Result> recallResult;
    private Map<String, Result> precisionResult;
    private Map<String, Result> coverageResult;
    private Map<String, Result> popularityResult;
    
    private void initDataModel() {
        L.i(this, "create data model");
        File file = new File(DATA_FILE);
        totalDataModel = null;
        try {
            totalDataModel = new MovielensModel(file);
        } catch (IOException e) {
            L.e(this, e);
            return;
        }
        L.i(this, "split data model");
        trainingDataModel = null;
        testDataModel = null;
        try {
            Pair<DataModel, DataModel> models =
                    DataModelUtils.split(totalDataModel, DataPercentage, 0.8);
            trainingDataModel = models.getFirst();
            testDataModel = models.getSecond();
        } catch (TasteException e) {
            L.e(this, e);
            return;
        }
    }
    
    private void initRecommender() {
        PearsonCorrelationSimilarity pearsonSimilarity = null;
        try {
            pearsonSimilarity = new PearsonCorrelationSimilarity(trainingDataModel);
        } catch (TasteException e) {
            L.e(this, e);
            return;
        }
        CachingItemSimilarity itemSimilarity = null;
        try {
            itemSimilarity = new CachingItemSimilarity(pearsonSimilarity, trainingDataModel);
        } catch (TasteException e) {
            L.e(this, e);
            return;
        }
        CachingUserSimilarity userSimilarity = null;
        try {
            userSimilarity = new CachingUserSimilarity(pearsonSimilarity, trainingDataModel);
        } catch (TasteException e) {
            L.e(this, e);
            return;
        }
        
        L.i(this, "build recommender");
        itemBasedRecommender = null;
        try {
            Recommender originRecommender= new GenericItemBasedRecommender(trainingDataModel,
                    itemSimilarity);
//            Recommender originRecommender = new KnnItemBasedRecommender(trainingDataModel,
//                    itemSimilarity, new NonNegativeQuadraticOptimizer(), 20);
            itemBasedRecommender = new PreCachingRecommender(originRecommender, AbsHitRateRunner.MAX_N);
        } catch (TasteException e) {
            L.e(this, e);
            return;
        }
        NearestNUserNeighborhood neighborhood = null;
        try {
//            neighborhood = new NearestNUserNeighborhood(20,
//                    userSimilarity, trainingDataModel);
            neighborhood = new NearestNUserNeighborhood(trainingDataModel.getNumUsers(),
                    userSimilarity, trainingDataModel);
        } catch (TasteException e) {
            L.e(this, e);
            return;
        }
        userBasedRecommender = null;
        try {
            Recommender originRecommender = new GenericUserBasedRecommender(trainingDataModel, neighborhood, userSimilarity);
            userBasedRecommender = new PreCachingRecommender(originRecommender, AbsHitRateRunner.MAX_N);
        } catch (TasteException e) {
            L.e(this, e);
            return;
        }
        randomRecommender = null;
        try {
            RandomRecommender originRecommender = new RandomRecommender(trainingDataModel);
            randomRecommender = new PreCachingRecommender(originRecommender, AbsHitRateRunner.MAX_N);
        } catch (TasteException e) {
            L.e(this, e);
            return;
        }
        itemAverageRecommender = null;
        try {
            ItemAverageRecommender originRecommender = new ItemAverageRecommender(trainingDataModel);
            itemAverageRecommender = new PreCachingRecommender(originRecommender, AbsHitRateRunner.MAX_N);
        } catch (TasteException e) {
            L.e(this, e);
            return;
        }
        slopeOneRecommender = null;
        try {
//            File diffFile = new File("slopeOne.diff");
//            diffFile.delete();
//            diffFile.createNewFile();
//            SlopeOneRecommender originRecommender = new SlopeOneRecommender(trainingDataModel, Weighting.WEIGHTED,
//                    Weighting.WEIGHTED, new FileDiffStorage(diffFile, Long.MAX_VALUE));
            SlopeOneRecommender originRecommender = new SlopeOneRecommender(trainingDataModel);
            slopeOneRecommender = new PreCachingRecommender(originRecommender, AbsHitRateRunner.MAX_N);
        } catch (Throwable e) {
            L.e(this, e);
            return;
        }
    }
    
    private void initEvaluator() {
        recallRateEvaluator = new RecallRateEvaluator();
        precisionRateEvaluator = new PrecisionRateEvaluator();
        coverageEvaluator = new CoverageEvaluator();
        popularityEvaluator = new PopularityEvaluator();
    }
    public void init() {
        initDataModel();
        initRecommender();
        initEvaluator();
    }
    
    public void exec() {
        recallResult = new HashMap<String, Result>();
        L.i(this, "\n\n******************* itemBased recall rate *****************\n\n");
        evaluateRecallRate(itemBasedRecommender, "itemBased");
        L.i(this, "\n\n******************* userBased recall rate *****************\n\n");
        evaluateRecallRate(userBasedRecommender, "userBased");
        L.i(this, "\n\n******************* random recommender recall rate *****************\n\n");
        evaluateRecallRate(randomRecommender, "random");
        L.i(this, "\n\n******************* average recommender recall rate *****************\n\n");
        evaluateRecallRate(itemAverageRecommender, "item average");
        L.i(this, "\n\n******************* slope one recommender recall rate *****************\n\n");
        evaluateRecallRate(slopeOneRecommender, "slope one");
        drawChart(recallResult, "recall rate", "recall rate", "recallRate.png", true);
        
        precisionResult = new HashMap<String, Result>();
        L.i(this, "\n\n******************* itemBased precision rate *****************\n\n");
        evaluatePrecisionRate(itemBasedRecommender, "itemBased");
        L.i(this, "\n\n******************* userBased precision rate *****************\n\n");
        evaluatePrecisionRate(userBasedRecommender, "userBased");
        L.i(this, "\n\n******************* random recommender precision rate *****************\n\n");
        evaluatePrecisionRate(randomRecommender, "random");
        L.i(this, "\n\n******************* average recommender precision rate *****************\n\n");
        evaluatePrecisionRate(itemAverageRecommender, "item average");
        L.i(this, "\n\n******************* slope one recommender precision rate *****************\n\n");
        evaluatePrecisionRate(slopeOneRecommender, "slope one");
        drawChart(precisionResult, "precision rate", "precision rate", "precisionRate.png", true);
        
        coverageResult = new HashMap<String, Result>();
        L.i(this, "\n\n******************* itemBased coverage rate *****************\n\n");
        evaluateCoverageRate(itemBasedRecommender, "itemBased");
        L.i(this, "\n\n******************* userBased coverage rate *****************\n\n");
        evaluateCoverageRate(userBasedRecommender, "userBased");
        L.i(this, "\n\n******************* random recommender coverage rate *****************\n\n");
        evaluateCoverageRate(randomRecommender, "random");
        L.i(this, "\n\n******************* average recommender coverage rate *****************\n\n");
        evaluateCoverageRate(itemAverageRecommender, "item average");
        L.i(this, "\n\n******************* slope one recommender coverage rate *****************\n\n");
        evaluateCoverageRate(slopeOneRecommender, "slope one");
        drawChart(coverageResult, "coverage rate", "coverage rate", "coverageRate.png", true);
        
        popularityResult = new HashMap<String, Result>();
        L.i(this, "\n\n******************* itemBased popularity *****************\n\n");
        evaluatePopularity(itemBasedRecommender, "itemBased");
        L.i(this, "\n\n******************* userBased popularity *****************\n\n");
        evaluatePopularity(userBasedRecommender, "userBased");
        L.i(this, "\n\n******************* random recommender popularity *****************\n\n");
        evaluatePopularity(randomRecommender, "random");
        L.i(this, "\n\n******************* average recommender popularity *****************\n\n");
        evaluatePopularity(itemAverageRecommender, "item average");
        L.i(this, "\n\n******************* slope one recommender popularity *****************\n\n");
        evaluatePopularity(slopeOneRecommender, "slope one");
        drawChart(popularityResult, "popularity", "polularity", "popularity.png", false);
    }
    
    private void evaluatePopularity(Recommender recommender, String resultName) {
        PopularityRunner runner = new PopularityRunner(popularityEvaluator, recommender,
                totalDataModel, testDataModel);
        execRunner(runner, popularityResult, resultName);
    }
    
    private void evaluateCoverageRate(Recommender recommender, String resultName) {
        CoverageRateRunner runner = new CoverageRateRunner(coverageEvaluator, recommender,
                totalDataModel, testDataModel);
        execRunner(runner, coverageResult, resultName);
    }
    
    private void evaluatePrecisionRate(Recommender recommender, String resultName) {
        PrecisionRateRunner runner = new PrecisionRateRunner(precisionRateEvaluator, recommender,
                totalDataModel, testDataModel);
        execRunner(runner, precisionResult, resultName);
    }
    
    private void evaluateRecallRate(Recommender recommender, String resultName) {
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
    
    private void drawChart(Map<String, Result> resultMap, String chartTitle, String yLabel, String imageFile
            , boolean withPercentage) {
        try {
            new ChartDrawer(chartTitle, yLabel, imageFile, resultMap, withPercentage).draw();
        } catch (IOException e) {
            L.e(this, e);
            return;
        }
    }

    public static void main(String[] args) {
        Evaluator evaluator = new Evaluator();
        evaluator.init();
        evaluator.exec();
    }
}
