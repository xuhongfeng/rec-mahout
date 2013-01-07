/**
 * @(#)Main.java, 2013-1-4. 
 * 
 */
package hongfeng.xu.rec.mahout;

import hongfeng.xu.rec.mahout.eval.ItemBasedRecommenderBuilder;
import hongfeng.xu.rec.mahout.eval.PrecisionRateEvaluator;
import hongfeng.xu.rec.mahout.eval.RecallRateEvaluator;
import hongfeng.xu.rec.mahout.model.MovielensModel;
import hongfeng.xu.rec.mahout.util.L;

import java.io.File;
import java.io.IOException;

import org.apache.mahout.cf.taste.common.TasteException;

/**
 * @author xuhongfeng
 *
 */
public class Main {
    public static void main(String[] args) {
        File dataFile = new File("data/u.data");
        MovielensModel dataModel = null;
        try {
            dataModel = new MovielensModel(dataFile);
        } catch (IOException e) {
            L.e("main", e);
            return;
        }
        
        ItemBasedRecommenderBuilder recommenderBuilder = new ItemBasedRecommenderBuilder();
        
        int N = 100;
        
        RecallRateEvaluator recallRateEvaluator = new RecallRateEvaluator();
        try {
            double recallRate = recallRateEvaluator.evaluate(recommenderBuilder,
                    dataModel, 0.8, 0.2, N);
            L.i("Main", "recall rate = %.2f%%", recallRate*100);
        } catch (TasteException e) {
            L.e("main", e);
            return;
        }
        
        PrecisionRateEvaluator precisionRateEvaluator = new PrecisionRateEvaluator();
        try {
            double precisionRate = precisionRateEvaluator.evaluate(recommenderBuilder,
                    dataModel, 0.8, 0.2, N);
            L.i("Main", "precision rate = %.2f%%", precisionRate*100);
        } catch (TasteException e) {
            L.e("main", e);
            return;
        }
    }
}
