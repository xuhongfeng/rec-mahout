/**
 * 2013-1-4
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author xuhongfeng
 *
 */
public class L {
    
    public static void e(Object tag, Throwable e) {
        LOG(tag).error("", e);
    }
    
    public static void e(Object tag, String msg) {
        LOG(tag).error(msg);;
    }
    public static void e(Object tag, String msg, Throwable e) {
        LOG(tag).error(msg, e);
    }
    
    private static Logger LOG(Object tag) {
        if (tag instanceof String) {
            String strTag = (String) tag;
            return LoggerFactory.getLogger(strTag);
        } else {
            return LoggerFactory.getLogger(tag.getClass());
        }
    }
}
