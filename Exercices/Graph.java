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

public class Graph {
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text couplekey = new Text();
    private ArrayList<String> lesAmis ;

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        lesAmis = value.toString.split() ;
        personneActuelle = lesAmis.remove(0) ;
        for (String ami : lesAmis) {
          ArrayList<String> lesAmisCopy = lesAmis.clone() ;
          lesAmisCopy.remove(ami) ;
          String autreAmis = lesAmisCopy.remove(0);
          String couple = "" ;
          if (personneActuelle.compareTo(ami)<=0){
            couple = "(" + personneActuelle + "," + ami + ")" ;
          }else{
            couple = "(" + ami + "," + personneActuelle + ")" ;
          }
          couplekey.set(new Text(couple));
          for (String autreAmi : lesAmisCopy){
            autreAmis = autreAmis + ";" + autreAmi ;
          }

          context.write(couplekey,new Text(autreAmis)) ;
        }
      }
    }

  public static class GraphReducer
       extends Reducer<Text,Text,Text,Text> {

    private Text result = new Text() ;
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      ArrayList<String> concat = new ArrayList<String>() ;
      for (Text value: ) {
        ArrayList<String> lesAmis = value.toString.split(";") ;
        for (String ami : lesAmis) {
          concat.add(ami) ;
        }
      }
      Collections.sort(concat);
      String precedent = "" ;
      String communs = "" ;
      int cpt = 0 ;
      for (String element: concat) {
        if (element.equals(precedent) {
          cpt ++ ;
          communs = communs + element ;
        }
        precedent = element ;
      }
      result.set(concat) ;
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Graph");
    job.setJarByClass(Graph.class);
    job.setMapperClass(TokenizerMapper.class);
    // job.setCombinerClass(PosteReducer.class);
    job.setReducerClass(GraphReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}