package com.nagpal.wordcount;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class WordCountMain {

    private static final Logger logger = Logger.getLogger(WordCountMain.class);
    private static final String OUTPUT_PATH = "/Users/bnagpal1/datasets/wordcount/output";
    public static final String INPUT_PATH = "/Users/bnagpal1/datasets/wordcount/word.txt";

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            throw new IllegalArgumentException("input and output path are required");
        }

        String inputPath = args[0];
        String outputPath = args[1];

        if (StringUtils.isBlank(inputPath)) {
            throw new IllegalArgumentException("Input path can't be empty");
        }

        if (StringUtils.isBlank(outputPath)) {
            throw new IllegalArgumentException("Output path can't be empty");
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Word Count");
        logger.info("Running word count...");

        FileSystem fs = FileSystem.get(new Configuration());
        // true stands for recursively deleting the folder you gave
        logger.info("Cleaning output directory...");
        fs.delete(new Path(outputPath), true);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setJarByClass(WordCountMain.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        boolean success = job.waitForCompletion(true);

        logger.info("Job status : " + (success ? "Passed" : "Failed"));
        System.exit(0);
    }
}
