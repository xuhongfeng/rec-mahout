/**
 * 2013-4-3
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.chart;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.jfree.chart.JFreeChart;
import org.jfree.data.general.AbstractDataset;

/**
 * @author xuhongfeng
 *
 */
public abstract class BaseDrawer<T extends BaseDrawer, DATA extends AbstractDataset> {
//    private static final int DEFAULT_WIDTH = 1280;
//    private static final int DEFAULT_HEIGHT = 800;
    private static final int DEFAULT_WIDTH = 800;
    private static final int DEFAULT_HEIGHT = 500;
    private static final String FORMAT = "png";
    
    protected DATA dataSet;
    protected String chartTitle = "";
    protected String xLabel = "";
    protected String yLabel = "";
    protected String imgFile = "img/default";
    protected boolean percentageFormat = false;
    protected boolean integerValue = false;
    protected int width = DEFAULT_WIDTH;
    protected int height = DEFAULT_HEIGHT;
    
    
    protected DoubleFormater doubleFormater = new DoubleFormater() {
        @Override
        public String format(double value) {
            if (integerValue) {
                int v = (int) value;
                return String.valueOf(v);
            }
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
    
    public BaseDrawer() {
        super();
        init();
    }
    
    protected void init() {
        dataSet = createDataSet();
    }

    public T draw() throws IOException {
        JFreeChart chart = createChart();
        chart.setBackgroundPaint(Color.WHITE);
        chart.getPlot().setBackgroundPaint(Color.WHITE);
        BufferedImage image = chart.createBufferedImage(width, height);
        ImageIO.write(image, FORMAT, new File(imgFile));
        return (T) this;
    }
    
    protected abstract JFreeChart createChart();
    protected abstract DATA createDataSet();
    
    public T setTitle(String title) {
        this.chartTitle = title;
        return (T) this;
    }
    
    public T setXLabel(String label) {
        this.xLabel = label;
        return (T) this;
    }
    
    public T setYLabel(String label) {
        this.yLabel = label;
        return (T) this;
    }
    
    public T setOutputFile(String imgFile) {
        this.imgFile = imgFile;
        return (T) this;
    }
    
    public static interface LongFormater {
        public String format(Long value);
    }
    
    public static interface DoubleFormater {
        public String format(double value);
    }
    
    public T setPercentageFormat(boolean isPercentage) {
        percentageFormat = isPercentage;
        return (T) this;
    }
    
    public T setIntegerValue(boolean isInteger) {
        integerValue = isInteger;
        return (T) this;
    }

    public T setWidth(int width) {
        this.width = width;
        return (T) this;
    }

    public T setHeight(int height) {
        this.height = height;
        return (T) this;
    }
    
}