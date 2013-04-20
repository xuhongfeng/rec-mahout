/**
 * 2013-4-16
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout;

import hongfeng.xu.rec.mahout.chart.BarChartDrawer;
import hongfeng.xu.rec.mahout.chart.XYChartDrawer;
import hongfeng.xu.rec.mahout.config.DataSetConfig;
import hongfeng.xu.rec.mahout.hadoop.BaseJob;
import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;
import hongfeng.xu.rec.mahout.util.L;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterator;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * @author xuhongfeng
 *
 */
public class Analyzer extends BaseJob {

    @Override
    protected int innerRun() throws Exception {
        
        itemBasedSimilarity();
        
        return 0;
    }
        
    
    private void itemBasedSimilarity() throws IOException {
        double start = 0.0, end = 1.0, step=0.1;
        Map<Double, Integer> map = new TreeMap<Double, Integer>();
        for (double i=start; i<=end; i+=step) {
            map.put(i, 0);
        }
        SequenceFileDirIterator<IntWritable, VectorWritable> it
            = HadoopHelper.openVectorIterator(new Path(DataSetConfig.getItemSimilarityPath()
                    , "rowVector"), getConf());
        while (it.hasNext()) {
            Pair<IntWritable, VectorWritable> pair = it.next();
            int userId = pair.getFirst().get();
            Vector vector = pair.getSecond().get();
            Iterator<Vector.Element> vectorIterator = vector.iterator();
            while (vectorIterator.hasNext()) {
                Vector.Element e = vectorIterator.next();
                if (e.index() > userId) {
                    double similarity = e.get();
                    for (double key:map.keySet()) {
                        if (similarity <= key) {
                            map.put(key, map.get(key)+1);
                            break;
                        }
                    }
                }
            }
        }
        it.close();
        
        BarChartDrawer drawer = new BarChartDrawer()
            .setIntegerValue(true)
            .setOutputFile("img/others/bar_item_similarity.png")
            .setXLabel("similarity range")
            .setYLabel("count")
            .setWidth(600)
            .setHeight(300);
        for (double i=start; i<=end; i+=step) {
            drawer.addSeries(String.valueOf(i), map.get(i));
        }
        drawer.draw();
    }
    
    private void userBasedSimilarity() throws IOException {
        double start = 0.0, end = 1.0, step=0.1;
        Map<Double, Integer> map = new TreeMap<Double, Integer>();
        for (double i=start; i<=end; i+=step) {
            map.put(i, 0);
        }
        SequenceFileDirIterator<IntWritable, VectorWritable> it
            = HadoopHelper.openVectorIterator(new Path(DataSetConfig.getUserSimilarityPath()
                    , "rowVector"), getConf());
        while (it.hasNext()) {
            Pair<IntWritable, VectorWritable> pair = it.next();
            int userId = pair.getFirst().get();
            Vector vector = pair.getSecond().get();
            Iterator<Vector.Element> vectorIterator = vector.iterator();
            while (vectorIterator.hasNext()) {
                Vector.Element e = vectorIterator.next();
                if (e.index() > userId) {
                    double similarity = e.get();
                    for (double key:map.keySet()) {
                        if (similarity <= key) {
                            map.put(key, map.get(key)+1);
                            break;
                        }
                    }
                }
            }
        }
        it.close();
        
        BarChartDrawer drawer = new BarChartDrawer()
            .setIntegerValue(true)
            .setOutputFile("img/others/bar_user_similarity.png")
            .setXLabel("similarity range")
            .setYLabel("count")
            .setWidth(600)
            .setHeight(300);
        for (double i=start; i<=end; i+=step) {
            drawer.addSeries(String.valueOf(i), map.get(i));
        }
        drawer.draw();
    }
        
    private void rateTrend() throws IOException {
        Map<Integer, Integer> map = new TreeMap<Integer, Integer>();
        SequenceFileDirIterator<IntWritable, VectorWritable> it
            = HadoopHelper.openVectorIterator(DataSetConfig.getUserItemVectorPath(), getConf());
        while (it.hasNext()) {
            Pair<IntWritable, VectorWritable> pair = it.next();
            int count = (int) pair.getSecond().get().zSum();
            if (map.containsKey(count)) {
                map.put(count, map.get(count)+1);
            } else {
                map.put(count, 1);
            }
        }
        it.close();
        
        int max = -1;
        for (int key:map.keySet()) {
            if (key > max) {
                max = key;
            }
        }
        double[][] values = new double[2][max+1];
        int sum = 0;
        for (int i=0; i<=max; i++) {
            if (map.containsKey(i)) {
                sum += map.get(i);
            }
            values[0][i]=i;
            values[1][i]=sum;
        }
        new XYChartDrawer().setTitle("")
            .setXLabel("Rate Count")
            .setYLabel("User Number")
            .setOutputFile("img/others/rate_trend.png")
            .setIntegerValue(true)
            .addSeries("", values)
            .draw();
    }
    
    private void rateCountBar() throws IOException {
        Map<Integer, Integer> map = new TreeMap<Integer, Integer>();
        int start=20, end=410, step=30;
        int otherCount = 0;
        for (int i=start; i<=end; i+=step) {
            map.put(i, 0);
        }
        
        SequenceFileDirIterator<IntWritable, VectorWritable> it
            = HadoopHelper.openVectorIterator(DataSetConfig.getUserItemVectorPath(), getConf());
        while (it.hasNext()) {
            Pair<IntWritable, VectorWritable> pair = it.next();
            int count = (int) pair.getSecond().get().zSum();
            if (count > end) {
                otherCount++;
            } else {
                for (int i=start; i<=end; i+=step) {
                    if (count<=i) {
                        map.put(i, map.get(i)+1);
                        break;
                    }
                }
            }
        }
        it.close();
        
        BarChartDrawer drawer = new BarChartDrawer()
            .setIntegerValue(true)
            .setOutputFile("img/others/rate_count_distribution_bar.png")
            .setXLabel("range")
            .setYLabel("count")
            .setWidth(600)
            .setHeight(300);
        for (int i=start; i<=end; i+=step) {
            drawer.addSeries(String.valueOf(i), map.get(i));
        }
        drawer.addSeries(">"+end, otherCount);
        drawer.draw();
    }
    
    private void rateCount() throws IOException {
        Map<Integer, Integer> map = new TreeMap<Integer, Integer>();
        SequenceFileDirIterator<IntWritable, VectorWritable> it
            = HadoopHelper.openVectorIterator(DataSetConfig.getUserItemVectorPath(), getConf());
        while (it.hasNext()) {
            Pair<IntWritable, VectorWritable> pair = it.next();
            int count = (int) pair.getSecond().get().zSum();
            if (map.containsKey(count)) {
                map.put(count, map.get(count)+1);
            } else {
                map.put(count, 1);
            }
        }
        it.close();
        double[][] values = new double[2][map.size()+1];
        values[0][0]=0;
        values[1][0]=0;
        int i=1;
        for (Map.Entry<Integer, Integer> e:map.entrySet()) {
            values[0][i] = e.getKey();
            values[1][i++] = e.getValue();
        }
        new XYChartDrawer().setTitle("")
            .setXLabel("Rate Count")
            .setYLabel("User Number")
            .setOutputFile("img/others/rate_count_distribution.png")
            .setIntegerValue(true)
            .addSeries("", values)
            .draw();
    }


    public static void main(String[] args) {
        Analyzer job = new Analyzer();
        try {
            ToolRunner.run(job, args);
        } catch (Exception e) {
            L.e(job, e);
        }
    }
}
