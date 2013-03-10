/**
 * 2013-3-10
 * 
 * xuhongfeng
 */
package hongfeng.xu.rec.mahout.hadoop;

import hongfeng.xu.rec.mahout.config.DeliciousDataConfig;
import hongfeng.xu.rec.mahout.hadoop.parser.RawDataParser;
import hongfeng.xu.rec.mahout.util.L;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.preparation.PreparePreferenceMatrixJob;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;

/**
 * @author xuhongfeng
 *
 */
public class EvaluateTagRecommender extends AbstractJob {
    
    public static final String OPTION_REUSE = "reuse";

    public static void main(String[] args) {
        EvaluateTagRecommender job = new EvaluateTagRecommender();
        try {
            ToolRunner.run(job, args);
        } catch (Exception e) {
            L.e(job, e);
        }
    }
    
    @Override
    public int run(String[] args) throws Exception {
        
        addInputOption();
        addOutputOption();
        
        addOption(buildOption(OPTION_REUSE, "r", "reuse the old parsed data or not", true, false, ""));

        Map<String,List<String>> parsedArgs = parseArguments(args);
        if (parsedArgs == null) {
          return -1;
        }
        
        AtomicInteger currentPhase = new AtomicInteger();
        
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            RawDataParser rawDataParser = new RawDataParser();
            ToolRunner.run(rawDataParser, new String[] {
                    "-i", getInputPath().toString(),
                    "-o", DeliciousDataConfig.HDFS_OUTPUT_DIR_RAW_DATA_PARSER,
                    "--" + OPTION_REUSE, getOption(OPTION_REUSE)
            });
        }
        
//        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
//            Path inputPath = RawDataParser.getUserItemPath();
//            Path outputPath = getItemIndexPath();
//            if (HadoopHelper.isFileExists(outputPath, getConf())) {
//                if (!hasOption(OPTION_REUSE) || !getOption(OPTION_REUSE).equals("true")) {
//                    HadoopUtil.delete(getConf(), outputPath);
//                    Job job= prepareJob(inputPath, outputPath, TextInputFormat.class,
//                            ItemIDIndexMapper.class, VarIntWritable.class, VarLongWritable.class,
//                            ItemIDIndexReducer.class, VarIntWritable.class, VarLongWritable.class,
//                            SequenceFileOutputFormat.class);
//                    job.setCombinerClass(ItemIDIndexReducer.class);
//                    if (!job.waitForCompletion(true)) {
//                        return -1;
//                    }
//                }
//            }
//        }
        
        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            Path inputPath = RawDataParser.getUserItemPath();
            Path outputPath = getUserItemMatrixPath();
            boolean needRun = true;
            if (HadoopHelper.isFileExists(outputPath, getConf())) {
                if (reuse()) {
                    needRun = false;
                } else {
                    HadoopUtil.delete(getConf(), outputPath);
                }
            }
            if (needRun) {
                ToolRunner.run(getConf(), new PreparePreferenceMatrixJob(), new String[]{
                    "--input", inputPath.toString(),
                    "--output", outputPath.toString(),
                    "--booleanData", String.valueOf(false),
                    "--tempDir", getTempPath().toString()});
            }
        }

        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
            boolean needRun = true;
            Path in = getRatingMatrix(getUserItemMatrixPath());
            Path out = getPopularItemPath();
            if (HadoopHelper.isFileExists(out, getConf())) {
                if (reuse()) {
                    needRun = false;
                } else {
                    HadoopUtil.delete(getConf(), out);
                }
            }
            if (needRun) {
                ToolRunner.run(getConf(), new PopularItemJob(), new String[]{
                    "--input", in.toString(),
                    "--output", out.toString(),
                });
            }
        }
        
//        if (shouldRunNextPhase(parsedArgs, currentPhase)) {
//            boolean needRun = true;
//            if (HadoopHelper.isFileExists(getPopularItemPath(), getConf())) {
//                if (reuse()) {
//                    needRun = false;
//                } else {
//                    HadoopUtil.delete(getConf(), getPopularItemPath());
//                }
//            }
//            if (needRun) {
//                sortPopularItems();
//            }
//        }
        
        return 0;
    }
    
//    private void sortPopularItems() throws IOException {
//        Path path = getRatingMatrix(getUserItemMatrixPath());
//        SequenceFile.Reader reader = new Reader(FileSystem.get(getConf()), path, getConf());
//        IntWritable index = new IntWritable();
//        VectorWritable prefVector = new VectorWritable(new RandomAccessSparseVector());
//        List<Pair<Integer, Double>> list = new ArrayList<Pair<Integer, Double>>();
//        while (reader.next(index, prefVector)) {
//            list.add(new Pair<Integer, Double>(index.get(), prefVector.get().zSum()));
//        }
//        Collections.sort(list, new Comparator<Pair<Integer, Double>>() {
//            @Override
//            public int compare(Pair<Integer, Double> o1,
//                    Pair<Integer, Double> o2) {
//                if (o1.getSecond() > o2.getSecond()) {
//                    return -1;
//                } else if (o1.getSecond() < o2.getSecond()) {
//                    return 1;
//                }
//                return 1;
//            }
//        });
//        reader.close();
//        
//        SequenceFile.Writer writer = new Writer(FileSystem.get(getConf()), getConf(),
//                getPopularItemPath(), IntWritable.class, DoubleWritable.class);
//        for (Pair<Integer, Double> pair:list) {
//            writer.append(new IntWritable(pair.getFirst()), new DoubleWritable(pair.getSecond()));
//        }
//        writer.close();
//    }
    
    private boolean reuse() {
        return hasOption(OPTION_REUSE) && getOption(OPTION_REUSE).equals("true");
    }
    
    public Path getPopularItemPath() {
        return new Path(new Path(DeliciousDataConfig.HDFS_DELICIOUS_DIR), "popularItem");
    }
    
//    public static Path getItemIndexPath() {
//        return new Path(DeliciousDataConfig.HDFS_DELICIOUS_DIR + "/itemIdIndex");
//    }
    
    public static Path getUserItemMatrixPath() {
        return new Path(DeliciousDataConfig.HDFS_DELICIOUS_DIR + "/userItemMatrix");
    }
    
    public static Path getRatingMatrix(Path parent) {
        return new Path(parent, "ratingMatrix");
    }
}
