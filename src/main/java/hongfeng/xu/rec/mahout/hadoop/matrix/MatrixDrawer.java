/**
 * 2013-3-30
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.matrix;

import hongfeng.xu.rec.mahout.hadoop.MultipleSequenceOutputFormat;

import java.awt.Color;
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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterator;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.title.TextTitle;
import org.jfree.chart.title.Title;
import org.jfree.data.xy.DefaultXYDataset;
import org.jfree.data.xy.XYDataset;

/**
 * @author xuhongfeng
 *
 */
public class MatrixDrawer {
    public static int WIDTH = 1280;
    public static int HEIGHT = 800;
    private static final String FORMAT = "png";
    
    private final Path[] inputPaths;
    private final String[] series;
    private final String outputFile;
    private final String title;
    private final float precesion;
    private final String[] subTitles;
    

    public MatrixDrawer(Path[] inputPaths, String[] series, String outputFile, String title
            , float precesion, String[] subTitles) {
        super();
        this.inputPaths = inputPaths;
        this.outputFile = outputFile;
        this.title = title;
        this.precesion = precesion;
        this.subTitles = subTitles;
        this.series = series;
    }

    public void draw(Configuration conf) throws IOException {
        DefaultXYDataset dataSet = new DefaultXYDataset();
        for (int i=0; i<inputPaths.length; i++) {
            Path inputPath = inputPaths[i];
            String serie = series[i];
            List<Pair<Double, Integer>> list = new ArrayList<Pair<Double,Integer>>();
            SequenceFileDirIterator<DoubleWritable, IntWritable> iterator =
                    new SequenceFileDirIterator<DoubleWritable, IntWritable>(
                            inputPath, PathType.LIST, MultipleSequenceOutputFormat.FILTER,
                            null, true, conf);
            while (iterator.hasNext()) {
                Pair<DoubleWritable, IntWritable> writablePair = iterator.next();
                Pair<Double, Integer> pair = new Pair<Double, Integer>(writablePair.getFirst().get(),
                        writablePair.getSecond().get());
                list.add(pair);
            }
            iterator.close();
            Collections.sort(list, new Comparator<Pair<Double, Integer>> () {
                @Override
                public int compare(Pair<Double, Integer> o1,
                        Pair<Double, Integer> o2) {
                    if (o1.getFirst() < o2.getFirst()) {
                        return -1;
                    }
                    return 1;
                }
            });
            updateDataSet(dataSet, serie, list);
        }
        JFreeChart chart = ChartFactory.createXYLineChart(title,
                "", "value", dataSet, PlotOrientation.VERTICAL, true, true, false);
        XYPlot plot = (XYPlot) chart.getPlot();
        chart.setBackgroundPaint(Color.WHITE);
        chart.getPlot().setBackgroundPaint(Color.WHITE);
        NumberAxis axis = (NumberAxis) plot.getRangeAxis();
        axis.setNumberFormatOverride(numberFormat);
        
        if (subTitles != null) {
            for (int i=0; i<subTitles.length; i++) {
                Title title = new TextTitle(subTitles[i]);
                chart.addSubtitle(i, title);
            }
        }
        
        BufferedImage image = chart.createBufferedImage(WIDTH, HEIGHT);
        ImageIO.write(image, FORMAT, new File(outputFile));
    }
    
    private XYDataset updateDataSet(DefaultXYDataset dataSet, String series, List<Pair<Double, Integer>> list) {
        int i=0;
        int j=0;
        double[][] values = new double[2][2*list.size()];
        for (Pair<Double, Integer> pair:list) {
            double v = pair.getFirst();
            int num = pair.getSecond();
            values[0][j] = Double.valueOf(i);
            values[1][j++] = v;
            i += num;
            values[0][j] = Double.valueOf(i-1);
            values[1][j++] = v;
        }
        dataSet.addSeries(series, values);
        
        return dataSet;
        
    }
    private NumberFormat numberFormat= new NumberFormat() {
        private static final long serialVersionUID = 7178959953024323658L;

        @Override
        public StringBuffer format(double number, StringBuffer toAppendTo,
                FieldPosition pos) {
            int count = (int) Math.log10(1/precesion);
            String format = "%." + count + "f";
            return new StringBuffer(String.format(format, number));
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
