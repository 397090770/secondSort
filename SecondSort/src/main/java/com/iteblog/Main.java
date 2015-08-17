package com.iteblog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * Created by yangping.wu on 2015-08-04.
 */
public class Main {

    public static void main(String[] args) throws Exception {
        // Make sure there are exactly 2 parameters
        if (args.length != 2) {
            System.out.println("SecondarySort <input-dir> <output-dir>");
            throw new IllegalArgumentException("SecondarySort <input-dir> <output-dir>");
        }


        Configuration conf = new Configuration();
        conf.set("mapreduce.job.queuename", "datadev");
        Job job = Job.getInstance(conf);
        job.setJarByClass(Main.class);
        job.setJobName("SecondarySort");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(Entry.class);
        job.setOutputValueClass(Text.class);


        job.setMapperClass(SecondarySortMapper.class);
        job.setReducerClass(SecondarySortReducer.class);
        job.setPartitionerClass(EntryPartitioner.class);
        job.setGroupingComparatorClass(EntryGroupingComparator.class);
        //job.setCombinerClass(SecondarySortReducer.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
