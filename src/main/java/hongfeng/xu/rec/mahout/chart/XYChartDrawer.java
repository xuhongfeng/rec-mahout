/**
 * 2013-4-3
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.chart;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.text.FieldPosition;
import java.text.NumberFormat;
import java.text.ParsePosition;

import javax.imageio.ImageIO;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.data.xy.DefaultXYDataset;

/**
 * @author xuhongfeng
 *
 */
public class XYChartDrawer {
    private static final int WIDTH = 1280;
    private static final int HEIGHT = 800;
    private static final String FORMAT = "png";
    
    protected final DefaultXYDataset dataSet;
    protected String chartTitle = "";
    protected String xLabel = "";
    protected String yLabel = "";
    protected String imgFile = "img/default";
    protected boolean percentageFormat = false;
    
    
    protected DoubleFormater doubleFormater = new DoubleFormater() {
        @Override
        public String format(double value) {
            if (percentageFormat) {
                return String.format("%.2f%%", value*100);
            } else {
                return String.format("%.2f", value);
            }
        }
    };
    
    protected LongFormater longFormater = new LongFormater() {
        @Override
        public String format(Long value) {
            return String.format("%.d", value);
        }
    };
    
    public XYChartDrawer() {
        super();
        dataSet = createData();
    }

    public void draw() throws IOException {
        JFreeChart chart = ChartFactory.createXYLineChart(chartTitle,
                xLabel, yLabel, dataSet, PlotOrientation.VERTICAL, true, true, false);
        XYPlot plot = (XYPlot) chart.getPlot();
        NumberAxis axis = (NumberAxis) plot.getRangeAxis();
        axis.setNumberFormatOverride(new NumberFormat() {
            @Override
            public StringBuffer format(double number, StringBuffer toAppendTo,
                    FieldPosition pos) {
                return new StringBuffer(doubleFormater.format(number));
            }
            @Override
            public StringBuffer format(long number, StringBuffer toAppendTo,
                    FieldPosition pos) {
                return new StringBuffer(longFormater.format(number));
            }
            @Override
            public Number parse(String source, ParsePosition parsePosition) {
                return null;
            }
            
        });
        
        BufferedImage image = chart.createBufferedImage(WIDTH, HEIGHT);
        ImageIO.write(image, FORMAT, new File(imgFile));
    }
    
    public XYChartDrawer addSeries(String title, double[][] values) {
        dataSet.addSeries(title, values);
        return this;
    }
    
    public XYChartDrawer setTitle(String title) {
        this.chartTitle = title;
        return this;
    }
    
    public XYChartDrawer setXLabel(String label) {
        this.xLabel = label;
        return this;
    }
    
    public XYChartDrawer setYLabel(String label) {
        this.yLabel = label;
        return this;
    }
    
    public XYChartDrawer setOutputFile(String imgFile) {
        this.imgFile = imgFile;
        return this;
    }
    
    protected DefaultXYDataset createData() {
        DefaultXYDataset dataSet = new DefaultXYDataset();
        return dataSet;
    }
    
    public static interface LongFormater {
        public String format(Long value);
    }
    
    public static interface DoubleFormater {
        public String format(double value);
    }
    
    public void setPercentageFormat(boolean isPercentage) {
        percentageFormat = isPercentage;
    }
}
