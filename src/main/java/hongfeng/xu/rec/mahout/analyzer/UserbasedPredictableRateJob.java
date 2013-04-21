/**
 * 2013-4-20
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.analyzer;

import hongfeng.xu.rec.mahout.config.DataSetConfig;
import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;
import hongfeng.xu.rec.mahout.hadoop.matrix.VectorCache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;

/**
 * @author xuhongfeng
 *
 */
public class UserbasedPredictableRateJob extends BasePredictableRateJob {
    
    public UserbasedPredictableRateJob(int[] kList) {
        super(kList);
    }
    
    @Override
    protected Class<? extends PredictableRateMapper> getMapperClass() {
        return MyMapper.class;
    }

    public static class MyMapper extends PredictableRateMapper {
        protected VectorCache uiCache;

        public MyMapper() {
            super();
        }
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            int userCount = HadoopUtil.readInt(DataSetConfig.getUserCountPath(),
                    context.getConfiguration());
            int itemCount = HadoopUtil.readInt(DataSetConfig.getItemCountPath(),
                    context.getConfiguration());
            uiCache = VectorCache.create(userCount, itemCount,
                    DataSetConfig.getUserItemVectorPath(), context.getConfiguration());
        }
        
        @Override
        protected void map(IntWritable key, VectorWritable value, Context context)
                throws IOException, InterruptedException {
            queue.clear();
            Vector simVector = value.get();
            int userId = key.get();
            Vector uiVector = uiCache.get(userId);
            Iterator<Element> it = simVector.iterator();
            while (it.hasNext()) {
                Element e = it.next();
                int otherUserId = e.index();
                double sim = e.get();
                if (otherUserId != userId) {
                    int count = 0;
                    if (sim != 0.0) {
                        Vector otherUiVector = uiCache.get(otherUserId);
                        count = HadoopHelper.sub(otherUiVector, uiVector);
                    }
                    Pair<Double, Integer> pair = new Pair<Double, Integer>(sim, count);
                    queue.add(pair);
                }
            }
            List<Pair<Double, Integer>> list = new ArrayList<Pair<Double, Integer>>();
            while (!queue.isEmpty()) {
                Pair<Double, Integer> pair = queue.poll();
                list.add(0, pair);
            }
            for (int k:KList) {
                double r = 0.0;
                double s = 0.0;
                for (int i=0; i<k; i++) {
                    Pair<Double, Integer> pair = list.get(i);
                    int total = uiVector.size() - uiVector.getNumNondefaultElements() - 1;
                    r += pair.getFirst()*pair.getSecond()/total;
                    s += pair.getFirst();
                }
                double rate = r/s;
                keyWritable.set(k);
                valueWritable.set(rate);
                HadoopHelper.log(this, "map "+ keyWritable.get() + ", " + valueWritable.get());
                context.write(keyWritable, valueWritable);
            }
        }
    }
}
