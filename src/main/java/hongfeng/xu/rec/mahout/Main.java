/**
 * @(#)Main.java, 2013-1-4. 
 * 
 */
package hongfeng.xu.rec.mahout;

import hongfeng.xu.rec.mahout.util.L;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;



/**
 * @author xuhongfeng
 *
 */
public class Main {
    public static void main(String[] args) {
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        dataset.addValue(1, "s1", "r1");
        dataset.addValue(2, "s1", "r2");
        JFreeChart chart = ChartFactory.createLineChart("title", "categoryAxisLabel",
                "valueAxisLabel", dataset, PlotOrientation.VERTICAL, true, true, false);
        BufferedImage image = chart.createBufferedImage(500, 300);
        try {
            ImageIO.write(image, "png", new File("test.png"));
        } catch (IOException e) {
            L.e("main", e);
        }
    }
}
