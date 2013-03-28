/**
 * 2013-3-26
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.misc;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;

/**
 * @author xuhongfeng
 *
 */
public class Line {
    public final long userId;
    public final long itemId;
    public final long tagId;
    public final long timestamp;
    
    public Line(long userId, long itemId, long tagId, long timestamp) {
        super();
        this.userId = userId;
        this.itemId = itemId;
        this.tagId = tagId;
        this.timestamp = timestamp;
    }
    
    @Override
    public String toString() {
        return String.format("%d\t%d\t%d\t%d", userId, itemId, tagId, timestamp);
    }
    
    public static Line parse(String line) {
        String[] ss = line.split("\\s");
        long userId = Long.valueOf(ss[0]);
        long itemId = Long.valueOf(ss[1]);
        long tagId = Long.valueOf(ss[2]);
        long timestamp = Long.valueOf(ss[3]);
        
        return new Line(userId, itemId, tagId, timestamp);
    }
    
    public static List<Line> parse(File file) throws IOException {
        List<String> lines = FileUtils.readLines(file);
        lines.remove(0);//ignore first header line
        List<Line> ret = new ArrayList<Line>(lines.size());
        for (String line:lines) {
            ret.add(Line.parse(line));
        }
        return ret;
    }
}
