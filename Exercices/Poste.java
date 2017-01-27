import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Poste {
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text villekey = new Text();


    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        String wordstr = value.toString() ;
        String[] wordSplit = wordstr.split(";");
        System.out.println(wordSplit);
        String ville = wordSplit[8] ;
        String wgs84 = wordSplit[10] ;
        villekey.set(ville) ;
        context.write(villekey, new Text(wgs84));
      }
    }

  public static class PosteReducer
       extends Reducer<Text,Text,Text,Text> {

    private Text result = new Text() ;
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      
      String concat = "" ;
      int sum = 0;
      for (Text val : values) {
        sum++;
        concat = concat +"(" + val.toString() + ")" ; 
      }
      concat = "(" + sum + ") " + concat ;
      result.set(concat) ;
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Poste");
    job.setJarByClass(Poste.class);
    job.setMapperClass(TokenizerMapper.class);
    // job.setCombinerClass(PosteReducer.class);
    job.setReducerClass(PosteReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}