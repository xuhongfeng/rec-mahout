/**
 * 2013-3-24
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.misc;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;

/**
 * @author xuhongfeng
 *
 */
public class ParseMovielens10M {
    public static void main(String[] args) {
        File input = new File("data/movielens-10m/tags.dat");
        File utiOut = new File("data/movielens-10m/user-tag-timestamp.dat");
        File tagOut = new File("data/movielens-10m/tagIds.dat");
        try {
            List<String> lines = FileUtils.readLines(input);
            List<String> uti = new ArrayList<String>();
            Map<Long, String> tagSet = new HashMap<Long, String>();
            for (String line:lines) {
                String[] ss = line.split("::");
                long userId = Long.valueOf(ss[0]);
                long itemId = Long.valueOf(ss[1]);
                String tag = ss[2];
                long tagId = hashTag(tag);
                long timestamp = Long.valueOf(ss[3]);
                
                tagSet.put(tagId, tag);
                
                uti.add(String.format("%d\t%d\t%d\t%d", userId, itemId, tagId, timestamp));
            }
            
            List<String> tagLines = new ArrayList<String>();
            for (Map.Entry<Long, String> entry:tagSet.entrySet()) {
                tagLines.add(String.format("%d\t%s", entry.getKey(), entry.getValue()));
            }
            
            FileUtils.writeLines(utiOut, uti);
            FileUtils.writeLines(tagOut, tagLines);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    private static long hashTag(String tag) {
        char[] chars = tag.toUpperCase().toCharArray();
        long hash = 1;
        for (char c:chars) {
            if (!Character.isSpaceChar(c)) {
                hash += hash*31 + c;
            }
        }
        return hash;
    }
}
