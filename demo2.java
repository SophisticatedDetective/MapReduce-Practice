package com.autohome.example;
import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TopKWords{
    public static final int K=50;
    public class KMap extends Mapper<LongWritable,Text,IntWritable,Text>{
        TreeMap<Integer,String> map=new TreeMap<Integer,String>();
        @override
        public static void map(LongWritable key,Text value,Context context) throws Exception{
            String line=value.toString();
            if(line.trim().length()>0 && line.indexOf("\t")!=-1){
                String[] arr=line.split("\t",2);
                String name=arr[0];
                Integer num=Integer.parseInt(arr[1]);
                map.put(num,name);
                if(map.size()>K){
                    map.remove(map.firstKey());
                }
            }
        }
        @override
        public void cleanUp(Mapper<LongWritable,Text,IntWritable,Text>.Context context) throws Exception{
            for(Integer num:map.keySet()){
                context.write(new IntWritable(num),new Text(map.get(num)));
            }            
        }
    }
    public class KReduce extends Reducer<IntWritable,Text,IntWritable,Text>{
        TreeMap<Integer,String> map=new TreeMap<Integer,String>();
        @override
        public static void reduce(IntWritable key,Iterable<Text> values,Context context) throws Exception{
            map.put(key.get(),values.iterator().next().toString());
            if(map.size()>K){
                map.remove(map.firstKey());
                
            }
        }
        public static void cleanUp(Reducer<IntWritable,Text,IntWritable,Text>.Context context) throws Exception{
            for(Integer num:map.keySet()){
                context.write(new IntWritable(num),new Text(map.get(num)));
            }
        }
    }
    public static void main(Srings[] args) throws Exception{
        Configuration conf=new Configuration();
        if(args.length!=2){
            System.err.println('There must be two arguments');
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "top Keyword");     
        job.setJarByClass(TopKeyword.class);  
        job.setMapperClass(KMap.class);  
        job.setCombinerClass(KReduce.class);  
        job.setReducerClass(KReduce.class);   
        job.setOutputKeyClass(IntWritable.class);  
        job.setOutputValueClass(Text.class);  
        job.setInputFormatClass(TextInputFormat.class); //这俩行行我写上了，但是不懂什么意思~
        job.setOutputFormatClass(TextOutputFormat.class); //       
        FileInputFormat.addInputPath(job, new Path(args[0]));  
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

