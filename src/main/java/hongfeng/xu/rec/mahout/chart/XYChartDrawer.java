/**
 * 2013-4-3 xuhongfeng
 */
package hongfeng.xu.rec.mahout.chart;

import java.text.FieldPosition;
import java.text.NumberFormat;
import java.text.ParsePosition;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.data.xy.DefaultXYDataset;

/**
 * @author xuhongfeng
 */
public class XYChartDrawer extends BaseDrawer<XYChartDrawer, DefaultXYDataset> {
    
    @Override
    protected JFreeChart createChart() {
        JFreeChart chart = ChartFactory.createXYLineChart(chartTitle, xLabel,
                yLabel, dataSet, PlotOrientation.VERTICAL, true, true, false);
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
        return chart;
    }
    
    public XYChartDrawer addSeries(String title, double[][] values) {
        dataSet.addSeries(title, values);
        return this;
    }
    
    @Override
    protected DefaultXYDataset createDataSet() {
        DefaultXYDataset dataSet = new DefaultXYDataset();
        return dataSet;
    }

}
