/**
 * 2013-4-7
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.lab;

import hongfeng.xu.rec.mahout.config.DataSetConfig;
import hongfeng.xu.rec.mahout.hadoop.BaseJob;
import hongfeng.xu.rec.mahout.hadoop.matrix.VectorCache;

import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.HadoopUtil;

/**
 * @author xuhongfeng
 *
 */
public class TestUnpredictRate extends BaseJob {

    public static void main(String[] args) {
    }

    @Override
    protected int innerRun() throws Exception {
        int userCount = HadoopUtil.readInt(DataSetConfig.getUserCountPath(), getConf());
        VectorCache cache = VectorCache.create(userCount, userCount,
                new Path(DataSetConfig.getUserBasedMatrix(), "rowVector"), getConf());
        return 0;
    }
}
