/**
 * 2013-4-16
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.chart;

import java.awt.Color;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.renderer.category.BarRenderer;
import org.jfree.data.category.DefaultCategoryDataset;

/**
 * @author xuhongfeng
 *
 */
public class BarChartDrawer extends BaseDrawer<BarChartDrawer, DefaultCategoryDataset> {

    @Override
    protected JFreeChart createChart() {
        JFreeChart chart = ChartFactory.createBarChart(chartTitle,
                xLabel, yLabel, dataSet, PlotOrientation.VERTICAL, false, false, false);
        
        CategoryPlot plot = (CategoryPlot) chart.getPlot();
        plot.setRenderer(barRenderer);
        return chart;
    }
    
    private BarRenderer barRenderer = new BarRenderer() {
        private static final long serialVersionUID = -284490038497242144L;

        public java.awt.Paint getItemPaint(int row, int column) {
            return Color.BLACK;
        };
    };
    
    public void addSeries(String name, int value) {
        dataSet.addValue(value, "", name);
    }

    @Override
    protected DefaultCategoryDataset createDataSet() {
        return new DefaultCategoryDataset();
    }

}
