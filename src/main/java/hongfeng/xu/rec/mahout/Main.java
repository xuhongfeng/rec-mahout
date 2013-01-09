/**
 * @(#)Main.java, 2013-1-4. 
 * 
 */
package hongfeng.xu.rec.mahout;

import hongfeng.xu.rec.mahout.eval.PrecisionRateEvaluator;
import hongfeng.xu.rec.mahout.eval.RecallRateEvaluator;
import hongfeng.xu.rec.mahout.model.MovielensModel;
import hongfeng.xu.rec.mahout.recommender.movielens.ItemBasedPearsonRecommender;
import hongfeng.xu.rec.mahout.runner.movielens.PrecisionRateRunner;
import hongfeng.xu.rec.mahout.runner.movielens.RecallRateRunner;
import hongfeng.xu.rec.mahout.util.DataModelUtils;
import hongfeng.xu.rec.mahout.util.L;

import java.io.File;
import java.io.IOException;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.common.Pair;


/**
 * @author xuhongfeng
 *
 */
public class Main {
    public static void main(String[] args) {
        
        /* step 1. create data model */
        L.i("Main", "create data model");
        File file = new File("data/u.data");
        MovielensModel totalDataModel = null;
        try {
            totalDataModel = new MovielensModel(file);
        } catch (IOException e) {
            L.e("main", e);
            return;
        }
        
        /* step 2. split data model */
        L.i("Main", "split data model");
        DataModel trainingDataModel = null;
        DataModel testDataModel = null;
        try {
            Pair<DataModel, DataModel> models =
                    DataModelUtils.split(totalDataModel, 1, 0.8);
            trainingDataModel = models.getFirst();
            testDataModel = models.getSecond();
        } catch (TasteException e) {
            L.e("main", e);
            return;
        }
        
        /* step 3. build Recommender */
        L.i("Main", "build recommender");
        ItemBasedPearsonRecommender recommender = null;
        try {
            recommender = new ItemBasedPearsonRecommender(trainingDataModel);
        } catch (TasteException e) {
            L.e("main", e);
            return;
        }
        
        /* step 4. evaluate recall rate */
        L.i("Main", "evaluate recall rate");
        RecallRateEvaluator recallRateEvaluator = new RecallRateEvaluator();
        new RecallRateRunner(recallRateEvaluator, recommender, testDataModel).exec();
        
        /* step 5. evaluate precision rate */
        L.i("Main", "evaluate precision rate");
        PrecisionRateEvaluator precisionRateEvaluator = new PrecisionRateEvaluator();
        new PrecisionRateRunner(precisionRateEvaluator, recommender, testDataModel).exec();
    }
}
