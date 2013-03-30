/**
 * 2013-3-29
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.threshold;

import hongfeng.xu.rec.mahout.hadoop.matrix.BaseMatrixJob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * @author xuhongfeng
 *
 */
public abstract class BaseThreshldMatrixJob extends BaseMatrixJob {
    protected final int threshold;
    
    public BaseThreshldMatrixJob(int n1, int n2, int n3, Path multiplyerPath
            , int threshold) {
        super(n1, n2, n3, multiplyerPath);
        this.threshold = threshold;
    }
    
    @Override
    protected void initConf(Configuration conf) {
        super.initConf(conf);
        conf.setInt("threshold", threshold);
    }
}
