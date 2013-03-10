/**
 * 2013-3-10
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.lab;

import hongfeng.xu.rec.mahout.config.DeliciousDataConfig;
import hongfeng.xu.rec.mahout.hadoop.parser.RawDataParser;

import org.apache.hadoop.fs.Path;

/**
 * @author xuhongfeng
 *
 */
public class TestRawDataParser {

    public static void main(String[] args) {
        Path dir = new Path(DeliciousDataConfig.HDFS_OUTPUT_DIR_RAW_DATA_PARSER);
        Path userItemPath = new Path(dir, RawDataParser.FILE_USER_ITEM);
        Path userTagPath = new Path(dir, RawDataParser.FILE_USER_TAG);
        Path itemTagPath= new Path(dir, RawDataParser.FILE_ITEM_TAG);
        Path testPath = new Path(dir, RawDataParser.FILE_TEST);
    }
}
