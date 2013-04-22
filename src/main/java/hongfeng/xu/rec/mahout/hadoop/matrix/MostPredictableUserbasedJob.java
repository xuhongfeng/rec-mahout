/**
 * 2013-4-21
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.matrix;

import hongfeng.xu.rec.mahout.config.DataSetConfig;
import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;
import hongfeng.xu.rec.mahout.structure.FixedSizePriorityQueue;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;

/**
 * @author xuhongfeng
 *
 */
public class MostPredictableUserbasedJob extends BaseMatrixJob {
    
    private final int k;

    public MostPredictableUserbasedJob(int n1, int n2, int n3,
            Path multiplyerPath, int k) {
        super(n1, n2, n3, multiplyerPath);
        this.k = k;
    }

    @Override
    protected Class<? extends MatrixReducer> getMatrixReducer() {
        return MyReducer.class;
    }
    
    @Override
    protected void initConf(Configuration conf) {
        super.initConf(conf);
        conf.setInt("k", k);
    }

    public static class MyReducer extends MatrixReducer {
        private int k;
        private FixedSizePriorityQueue<Pair<Double, Integer>> queue;
        private VectorCache uiCache;

        public MyReducer() {
            super();
        }
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            this.k = context.getConfiguration().getInt("k", -1);
            HadoopHelper.log(this, "k=" + k);
            if (k == -1) {
                throw new RuntimeException();
            }
            int userCount = HadoopUtil.readInt(DataSetConfig.getUserCountPath(), conf);
            int itemCount = HadoopUtil.readInt(DataSetConfig.getItemCountPath(), conf);
            uiCache = VectorCache.create(userCount, itemCount, DataSetConfig.getUserItemVectorPath(), conf);
            queue = new FixedSizePriorityQueue<Pair<Double, Integer>>(k,
                new Comparator<Pair<Double, Integer>>() {
                    @Override
                    public int compare(Pair<Double, Integer> o1,
                            Pair<Double, Integer> o2) {
                        if (o1.getFirst() > o2.getFirst()) {
                            return 1;
                        }
                        if (o1.getFirst() < o2.getFirst()) {
                            return -1;
                        }
                        return 0;
                    }
            });
        }

        @Override
        protected double calculate(int i, int j, Vector simVector, Vector iuVector) {
            int userId = i;
            Vector uiVector = uiCache.get(userId);
            queue.clear();
            Iterator<Element> simIt = simVector.iterateNonZero();
            while (simIt.hasNext()) {
                Element simEle = simIt.next();
                int otherUserId = simEle.index();
                double sim = simEle.get();
                Vector otherUiVector = uiCache.get(otherUserId);
                int sub = HadoopHelper.sub(otherUiVector, uiVector);
                queue.add(new Pair<Double, Integer>(sim*sub, otherUserId));
            }
            if (DataSetConfig.ONE_ZERO) {
                double total = 0.0;
                for (Pair<Double, Integer> pair:queue) {
                    int otherUserId = pair.getSecond();
                    double sim = simVector.get(otherUserId);
                    double pref = iuVector.get(otherUserId);
                    total += sim*pref;
                }
                return total;
            } else {
                throw new RuntimeException();
            }
        }
    }
}
