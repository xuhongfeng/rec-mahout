/**
 * 2013-4-3
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop.parser;

import hongfeng.xu.rec.mahout.chart.XYChartDrawer;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.HadoopUtil;

/**
 * @author xuhongfeng
 *
 */
public class DrawRawData {
    private final Path originData;
    private final Path processedData;
    private final Configuration conf;
    private final String title;
    
    public DrawRawData(Path originData, Path processedData, String title, Configuration conf) {
        super();
        this.originData = originData;
        this.processedData = processedData;
        this.conf = conf;
        this.title = title;
    }

    public void draw(String imageFile) throws IOException {
        XYChartDrawer drawer = new XYChartDrawer();
        drawer.setTitle(title).addSeries("origin", parseOriginValue())
            .setOutputFile(imageFile).draw();
    }
    
//    private double[][] parseProcessedValue() throws IOException {
//        int[] count = new int[] {0, 0};
//        double[][] values = new double[2][4];
//        SequenceFileDirIterator<IntIntWritable, DoubleWritable> iterator
//            = new SequenceFileDirIterator<IntIntWritable, DoubleWritable>(
//                    processedData, PathType.LIST, MultipleSequenceOutputFormat.FILTER,
//                    null, true, conf);
//        try {
//            while (iterator.hasNext()) {
//                Pair<IntIntWritable, DoubleWritable> pair= iterator.next();
//                if (pair.getSecond().get() == 1.0) {
//                    count[1] ++;
//                } else if (pair.getSecond().get() == -1.0) {
//                    count[0] ++;
//                } else {
//                    throw new RuntimeException();
//                }
//            }
//        } finally {
//            iterator.close();
//        }
//        values[0][0] = 0;
//        values[1][0] = -1;
//        values[0][1] = count[0];
//        values[1][1] = -1;
//        values[0][2] = count[0];
//        values[1][2] = 1;
//        values[0][3] = count[0] + count[1];
//        values[1][3] = 1;
//        
//        return values;
//    }
    
    private double[][] parseOriginValue() throws IOException {
        int[] count = new int[] {0, 0, 0, 0, 0};
        double[][] values = new double[2][10];
        InputStream is = HadoopUtil.openStream(originData, conf);
        try {
            List<String> lines = IOUtils.readLines(is);
            for (String line:lines) {
                String[] ss = line.split("\\s");
                int value = Integer.valueOf(ss[2]);
                count[value-1]++;
            }
        } finally {
            is.close();
        }
        int j=0;
        int k = 0;
        for (int i=0; i<5; i++) {
            values[0][j] = k;
            values[1][j++] = i+1;
            k += count[i];
            values[0][j] = k;
            values[1][j++] = i+1;
        }
        return values;
    }
}