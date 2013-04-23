/**
 * 2013-1-13
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.chart;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.text.FieldPosition;
import java.text.NumberFormat;
import java.text.ParsePosition;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.imageio.ImageIO;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;

/**
 * @author xuhongfeng
 *
 */
public class ChartDrawer {
    private static final int WIDTH = 1280;
    private static final int HEIGHT = 800;
    private static final String XLABEL = "N";
    private static final String FORMAT = "png";
    
    private final String title;
    private final String yLabel;
    private final String outputFile;
    private final Map<String, Result> resultMap;
    private final boolean withPercentage;
    
    public ChartDrawer(String title, String yLabel, String outputFile,
            Map<String, Result> resultMap, boolean withPercentage) {
        super();
        this.title = title;
        this.yLabel = yLabel;
        this.outputFile = outputFile;
        this.resultMap = resultMap;
        this.withPercentage = withPercentage;
    }

    public void draw() throws IOException {
        CategoryDataset dataSet = createDataSet();
        JFreeChart chart = ChartFactory.createLineChart(title, XLABEL, yLabel, 
                dataSet, PlotOrientation.VERTICAL, true, true, false);
        chart.setBackgroundPaint(Color.WHITE);
        chart.getPlot().setBackgroundPaint(Color.WHITE);
        CategoryPlot plot = (CategoryPlot) chart.getPlot();
        NumberAxis axis = (NumberAxis) plot.getRangeAxis();
        axis.setNumberFormatOverride(numberFormat);
        
        axis.setAutoRange(true);
        axis.setAutoRangeIncludesZero(false);
        
        BufferedImage image = chart.createBufferedImage(WIDTH, HEIGHT);
        ImageIO.write(image, FORMAT, new File(outputFile));
    }
    
    private NumberFormat numberFormat= new NumberFormat() {
        private static final long serialVersionUID = 7178959953024323658L;

        @Override
        public StringBuffer format(double number, StringBuffer toAppendTo,
                FieldPosition pos) {
            if (withPercentage) {
                return new StringBuffer(String.format("%.2f%%", number*100));
            } else {
                return new StringBuffer(String.format("%.2f", number));
            }
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
    
    private CategoryDataset createDataSet() {
        DefaultCategoryDataset dataSet = new DefaultCategoryDataset();
        Set<String> series = resultMap.keySet();
        for (String serie:series) {
            Result result = resultMap.get(serie);
            List<Integer> listN = result.listN();
            for (Integer N:listN) {
                double value = result.getValue(N);
                dataSet.addValue(value, serie, N);
            }
        }
        return dataSet;
    }
}
