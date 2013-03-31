/**
 * 2013-3-29
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.threshold;

import hongfeng.xu.rec.mahout.hadoop.MultipleSequenceOutputFormat;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.text.FieldPosition;
import java.text.NumberFormat;
import java.text.ParsePosition;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterator;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.data.xy.DefaultXYDataset;
import org.jfree.data.xy.XYDataset;

/**
 * @author xuhongfeng
 *
 */
public class DrawCountOneZero {
    private static final int WIDTH = 1280;
    private static final int HEIGHT = 800;
    private static final String FORMAT = "png";
    
    private final Path inputPath;
    private final String title;
    private final String imgFile;

    public DrawCountOneZero(Path inputPath, String title, String imgFile) {
        super();
        this.inputPath = inputPath;
        this.title = title;
        this.imgFile = imgFile;
    }

    public void draw(Configuration conf) throws IOException {
        List<Pair<Integer, Integer>> list = new ArrayList<Pair<Integer,Integer>>();
        SequenceFileDirIterator<IntWritable, IntWritable> iterator =
                new SequenceFileDirIterator<IntWritable, IntWritable>(
                        inputPath,
                        PathType.LIST, MultipleSequenceOutputFormat.FILTER,
                        null, true, conf);
        while (iterator.hasNext()) {
            Pair<IntWritable, IntWritable> writablePair = iterator.next();
            Pair<Integer, Integer> pair = new Pair<Integer, Integer>(writablePair.getFirst().get(),
                    writablePair.getSecond().get());
            list.add(pair);
        }
        iterator.close();
        Collections.sort(list, new Comparator<Pair<Integer, Integer>> () {
            @Override
            public int compare(Pair<Integer, Integer> o1,
                    Pair<Integer, Integer> o2) {
                if (o1.getFirst() < o2.getFirst()) {
                    return -1;
                }
                return 1;
            }
        });
        XYDataset dataSet = createDataSet(list);
        JFreeChart chart = ChartFactory.createXYLineChart(title,
                "", "count", dataSet, PlotOrientation.VERTICAL, true, true, false);
        XYPlot plot = (XYPlot) chart.getPlot();
        NumberAxis axis = (NumberAxis) plot.getRangeAxis();
        axis.setNumberFormatOverride(numberFormat);
        
        BufferedImage image = chart.createBufferedImage(WIDTH, HEIGHT);
        ImageIO.write(image, FORMAT, new File(imgFile));
    }
    
    private XYDataset createDataSet(List<Pair<Integer, Integer>> list) {
        DefaultXYDataset dataSet = new DefaultXYDataset();
        
        int i=0;
        int j=0;
        double[][] values = new double[2][2*list.size()];
        for (Pair<Integer, Integer> pair:list) {
            int count = pair.getFirst();
            int num = pair.getSecond();
            values[0][j] = Double.valueOf(i);
            values[1][j++] = Double.valueOf(count);
            i += num;
            values[0][j] = Double.valueOf(i-1);
            values[1][j++] = Double.valueOf(count);
        }
        dataSet.addSeries("", values);
        
        return dataSet;
        
    }
    private static final NumberFormat numberFormat= new NumberFormat() {
        private static final long serialVersionUID = 7178959953024323658L;

        @Override
        public StringBuffer format(double number, StringBuffer toAppendTo,
                FieldPosition pos) {
            return new StringBuffer(String.format("%d", (int)number));
        }

        @Override
        public StringBuffer format(long number, StringBuffer toAppendTo,
                FieldPosition pos) {
            return null;
        }

        @Override
        public Number parse(String source, ParsePosition parsePosition) {
            return null;
        }
    };
}
