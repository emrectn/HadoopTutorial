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

public class MapReduceRange {

//veri,output-key, output-value
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    
    public String classify(int beat){
        String result = null;
        if (beat>=0 && beat<=400)
            result = "0-0400";
        else if (beat>400 && beat<=800)
            result =  "0400-0800";
        else if (beat>800 && beat<=1200)
            result =  "0800-1200";
        else if (beat>1200 && beat<=1600)
            result =  "1200-1600";
        else if (beat>1600 && beat<=2000)
            result =  "1600-2000";
        else if (beat>2000)
            result =  "2000+";
        else
            result = "Error";
        return result;
    }

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

        String[] data = value.toString().split(",");
        try {
            context.write(new Text(classify(Integer.parseInt(data[10]))), one); 
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
    }
  }
//yukaridan alinan veriler
  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    // Hadoop Configration
    Configuration conf = new Configuration();
    // Job'a isim veriyoruz- Monitor ederken web arayuzden takip edecegimiz isim
    Job job = Job.getInstance(conf, "word count emre");
    // icinde bulundugumuz class adi - yukarida goruntuleyebiliriz.
    job.setJarByClass(MapReduceRange.class);

    // uc asamadan olusur bunlar. Mapper, Combiner, Reducer
    // Yukarida belirtilen classlari asagida setediyoruz
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);

    // Sonuc olarak emre:30 - key,value. Output key olarak bir text ve Output value olarak bir Int cikti uretmektedir.
    //Bunlar set edilir.
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    // Kelimelerini sayacagimiz input dosyasi
    FileInputFormat.addInputPath(job, new Path(args[0]));
    // Sonuclari yazacagimiz dosya
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    // Thread mantiginda calistigi icin threadlerin olmesini bekliyor
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
