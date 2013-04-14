/**
 * 2013-3-30
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.matrix;

import hongfeng.xu.rec.mahout.config.DataSetConfig;
import hongfeng.xu.rec.mahout.structure.FixedSizePriorityQueue;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.Pair;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;

/**
 * @author xuhongfeng
 *
 */
public class MultiplyNearestNeighborJob extends BaseMatrixJob {
    public static final int TYPE_FIRST = 0;
    public static final int TYPE_SECOND = TYPE_FIRST + 1;
    
    private final int type;
    private final int k;
    
    public MultiplyNearestNeighborJob(int n1, int n2, int n3,
            Path multiplyerPath, int type, int k) {
        super(n1, n2, n3, multiplyerPath);
        this.type = type;
        this.k = k;
    }
    
    @Override
    protected void initConf(Configuration conf) {
        super.initConf(conf);
        conf.setInt("type", type);
        conf.setInt("k", k);
    }

    @Override
    protected Class<? extends MatrixReducer> getMatrixReducer() {
        return MyReducer.class;
    }

    public static class MyReducer extends MatrixReducer {
        private int type;
        private int k;
        private FixedSizePriorityQueue<Pair<Double,Double>> queue;

        public MyReducer() {
            super();
        }
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            this.k = context.getConfiguration().getInt("k", 100);
            this.type = context.getConfiguration().getInt("type", TYPE_FIRST);
            queue = new FixedSizePriorityQueue<Pair<Double,Double>>(k,
                new Comparator<Pair<Double, Double>>() {
                    @Override
                    public int compare(Pair<Double, Double> o1,
                            Pair<Double, Double> o2) {
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
        protected double calculate(int i, int j, Vector vector1, Vector vector2) {
            if (i == j) {
                return 0.0;
            }
            queue.clear();
            if (type == TYPE_FIRST) {
                Iterator<Element> iterator = vector1.iterateNonZero();
                while (iterator.hasNext()) {
                    Element e = iterator.next();
                    double pref = vector2.getQuick(e.index());
                    double sim = e.get();
                    Pair<Double, Double> pair = 
                            new Pair<Double, Double>(sim, pref);
                    queue.add(pair);
                }
            } else {
                Iterator<Element> iterator = vector2.iterateNonZero();
                while (iterator.hasNext()) {
                    Element e = iterator.next();
                    double pref = vector1.getQuick(e.index());
                    double sim = e.get();
                    Pair<Double, Double> pair = 
                            new Pair<Double, Double>(sim, pref);
                    queue.add(pair);
                }
            }
            if (DataSetConfig.ONE_ZERO) {
                double totalSim = 0.0;
                for (Pair<Double, Double> pair:queue) {
                    totalSim += pair.getFirst()*pair.getSecond();
                }
                return totalSim;
            } else {
                double v = 0.0;
                double c = 0.0;
                for (Pair<Double, Double> pair:queue) {
                    double pref = pair.getSecond();
                    if (pref == 0.0) {
                        continue;
                    }
                    double sim = pair.getFirst();
                    v += sim*pref;
                    c += Math.abs(sim);
                }
                return c==0.0?0.0:v/c;
            }
        }
    }
    
}
