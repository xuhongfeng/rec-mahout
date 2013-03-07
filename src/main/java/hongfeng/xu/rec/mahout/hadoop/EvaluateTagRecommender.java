/**
 * 2013-3-10
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop;

import hongfeng.xu.rec.mahout.util.L;

import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;

/**
 * @author xuhongfeng
 *
 */
public class EvaluateTagRecommender extends AbstractJob {

    public static void main(String[] args) {
        EvaluateTagRecommender job = new EvaluateTagRecommender();
        try {
            ToolRunner.run(job, args);
        } catch (Exception e) {
            L.e(job, e);
        }
    }
    
    @Override
    public int run(String[] args) throws Exception {
        return 0;
    }
    
}
