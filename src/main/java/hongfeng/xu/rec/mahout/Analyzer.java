/**
 * 2013-4-16
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout;

import hongfeng.xu.rec.mahout.analyzer.ItembasedPredictableRateJob;
import hongfeng.xu.rec.mahout.analyzer.UserbasedPredictableRateJob;
import hongfeng.xu.rec.mahout.chart.BarChartDrawer;
import hongfeng.xu.rec.mahout.chart.XYChartDrawer;
import hongfeng.xu.rec.mahout.config.DataSetConfig;
import hongfeng.xu.rec.mahout.hadoop.BaseJob;
import hongfeng.xu.rec.mahout.hadoop.HadoopHelper;
import hongfeng.xu.rec.mahout.hadoop.matrix.DrawMatrixJob;
import hongfeng.xu.rec.mahout.hadoop.matrix.MatrixDrawer;
import hongfeng.xu.rec.mahout.hadoop.misc.ComputeIntersectJob;
import hongfeng.xu.rec.mahout.util.L;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
    private static final int WIDTH = 600;
    private static final int HEIGHT = 300;

    @Override
    protected int innerRun() throws Exception {
        MatrixDrawer.WIDTH = WIDTH;
        MatrixDrawer.HEIGHT = HEIGHT;
        
//        calPredictableRate();
        
        drawIntersect();
        
        return 0;
    }
    
    private void calPredictableRate() throws Exception {
        calUserBasedPredictable(new Path(DataSetConfig.getUserSimilarityPath(), "rowVector"),
                DataSetConfig.getPredictableRateOriginUser(),
                "UserBased Predictable Rate",
                "img/others/predictable-user-based.png");
        calItemBasedPredictable(new Path(DataSetConfig.getItemSimilarityPath(), "rowVector"),
                DataSetConfig.getPredictableRateOriginItem(),
                "ItemBased Predictable Rate",
                "img/others/predictable-item-based.png");
    }
    
    private void calUserBasedPredictable(Path uuPath, Path output, String title, String imageFile) throws Exception {
        int[] KList = new int[]{10, 20, 50, 80, 100, 150, 200, 250, 300, 350,
                400, 450, 500, 550, 600, 650, 700, 750, 800, 850, 900, 942};
        UserbasedPredictableRateJob job = new UserbasedPredictableRateJob(KList);
        runJob(job, uuPath, output, true);
        
        XYChartDrawer drawer = new XYChartDrawer();
        drawer.setPercentageFormat(true);
        drawer.setXLabel("k");
        drawer.setYLabel("rate");
        drawer.setTitle(title);
        drawer.setOutputFile(imageFile);
        drawer.setWidth(WIDTH);
        drawer.setHeight(HEIGHT);
        double[][] values = new double[2][KList.length];
        for (int i=0; i<KList.length; i++) {
            values[0][i] = KList[i];
        }
        SequenceFileDirIterator<IntWritable, DoubleWritable> it =
                open(IntWritable.class, DoubleWritable.class, output);
        while (it.hasNext()) {
            Pair<IntWritable, DoubleWritable> pair = it.next();
            int k = pair.getFirst().get();
            double value = pair.getSecond().get();
            for (int i=0; i<KList.length; i++) {
                if (values[0][i] == k) {
                    values[1][i] = value;
                    HadoopHelper.log(this, "k="+k+", value="+value);
                    break;
                }
            }
        }
        it.close();
        drawer.addSeries("", values);
        drawer.draw();
    }
    
    private void calItemBasedPredictable(Path iiPath, Path output, String title, String imgFile) throws Exception {
        int[] KList = new int[]{150, 180, 200,
                230, 250, 280, 300, 350,
                400, 450, 500, 550, 600, 650, 700, 750, 800, 850, 900, 950
                , 1000, 1100, 1200, 1300, 1400, 1500};
        ItembasedPredictableRateJob job = new ItembasedPredictableRateJob(KList, iiPath);
        runJob(job, DataSetConfig.getUserItemVectorPath(), output, true);
        
        XYChartDrawer drawer = new XYChartDrawer();
        drawer.setPercentageFormat(true);
        drawer.setXLabel("k");
        drawer.setYLabel("rate");
        drawer.setTitle(title);
        drawer.setOutputFile(imgFile);
        drawer.setWidth(WIDTH);
        drawer.setHeight(HEIGHT);
        double[][] values = new double[2][KList.length];
        for (int i=0; i<KList.length; i++) {
            values[0][i] = KList[i];
        }
        SequenceFileDirIterator<IntWritable, DoubleWritable> it =
                open(IntWritable.class, DoubleWritable.class, output);
        while (it.hasNext()) {
            Pair<IntWritable, DoubleWritable> pair = it.next();
            int k = pair.getFirst().get();
            double value = pair.getSecond().get();
            for (int i=0; i<KList.length; i++) {
                if (values[0][i] == k) {
                    values[1][i] = value;
                    HadoopHelper.log(this, "k="+k+", value="+value);
                    break;
                }
            }
        }
        it.close();
        drawer.addSeries("", values);
        drawer.draw();
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

    private void drawIntersect() throws Exception {

        ComputeIntersectJob computeIntersectJob = new ComputeIntersectJob(
                userCount(), itemCount(), userCount(),
                DataSetConfig.getUserItemVectorPath());
        runJob(computeIntersectJob, DataSetConfig.getUserItemVectorPath(),
                DataSetConfig.getUserIntersectPath(), true);
        DrawMatrixJob drawIntersect = new DrawMatrixJob(0.1f,
                "img/others/intersect-user.png", "", new String[0], new Path[] {
                    DataSetConfig.getUserIntersectPath()
                }, new String[] {
                    "intersect"
                }, false, true);
        runJob(drawIntersect, new Path("test"), new Path("test"), false);

        computeIntersectJob = new ComputeIntersectJob(
                itemCount(), userCount(), itemCount(),
                DataSetConfig.getItemUserVectorPath());
        runJob(computeIntersectJob, DataSetConfig.getItemUserVectorPath(),
                DataSetConfig.getItemIntersectPath(), true);
        drawIntersect = new DrawMatrixJob(0.1f,
                "img/others/intersect-item.png", "", new String[0], new Path[] {
                    DataSetConfig.getItemIntersectPath()
                }, new String[] {
                    "intersect"
                }, false, true);
        runJob(drawIntersect, new Path("test"), new Path("test"), false);
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
