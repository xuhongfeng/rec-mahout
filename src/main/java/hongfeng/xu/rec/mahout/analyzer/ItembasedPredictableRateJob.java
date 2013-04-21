/**
 * 2013-4-21
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.analyzer;

import hongfeng.xu.rec.mahout.config.DataSetConfig;
import hongfeng.xu.rec.mahout.hadoop.matrix.VectorCache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
public class ItembasedPredictableRateJob extends BasePredictableRateJob {
    private final Path iiPath;
    
    public ItembasedPredictableRateJob(int[] kList, Path iiPath) {
        super(kList);
        this.iiPath = iiPath;
    }
    
    @Override
    protected void initConf(Configuration conf) {
        super.initConf(conf);
        conf.set("iiPath", iiPath.toString());
    }

    @Override
    protected Class<? extends PredictableRateMapper> getMapperClass() {
        return MyMapper.class;
    }

    public static class MyMapper extends PredictableRateMapper {
        private VectorCache iiCache;
        private Path iiPath;

        public MyMapper() {
            super();
        }
        
        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            super.setup(context);
            iiPath = new Path(context.getConfiguration().get("iiPath"));
            int itemCount = HadoopUtil.readInt(DataSetConfig.getItemCountPath(),
                    context.getConfiguration());
            iiCache = VectorCache.create(itemCount, itemCount, iiPath, context.getConfiguration());
        }
        
        @Override
        protected void map(IntWritable key, VectorWritable value, Context context)
                throws IOException, InterruptedException {
            Vector uiVector = value.get();
            
            double[] c = new double[KList.length];
            double[] t = new double[KList.length];
            for (int i=0; i<c.length; i++) {
                c[i] = 0;
                t[i] = 0;
            }
            
            Iterator<Element> uiIt = uiVector.iterator();
            while (uiIt.hasNext()) {
                Element e = uiIt.next();
                int itemId = e.index();
                double pref = e.get();
                if (pref == 0.0) {
                    queue.clear();
                    Vector iiVector = iiCache.get(itemId);
                    Iterator<Element> iiIt = iiVector.iterator();
                    while (iiIt.hasNext()) {
                        Element iiEle = iiIt.next();
                        double sim = iiEle.get();
                        int otherItemId = iiEle.index();
                        int count = uiVector.get(otherItemId)==0.0?0:1;
                        Pair<Double, Integer> pair = new Pair<Double, Integer>(sim, count);
                        queue.add(pair);
                    }
                    List<Pair<Double, Integer>> list = new ArrayList<Pair<Double, Integer>>();
                    while (!queue.isEmpty()) {
                        Pair<Double, Integer> pair = queue.poll();
                        list.add(0, pair);
                    }
                    for (int i=0; i<KList.length; i++) {
                        int k = KList[i];
                        double cc = 0;
                        double ss = 0;
                        for (int j=0; j<k; j++) {
                            Pair<Double, Integer> pair = list.get(j);
                            cc += pair.getFirst()*pair.getSecond();
                            ss += pair.getFirst();
                        }
                        c[i] += cc;
                        t[i] += ss;
                    }
                }
            }
            for (int i=0; i<KList.length; i++) {
                int k = KList[i];
                double rate = c[i]*1.0/t[i];
                keyWritable.set(k);
                valueWritable.set(rate);
                context.write(keyWritable, valueWritable);
            }
        }
    }
}
