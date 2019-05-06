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

public class Metin {

//veri,output-key, output-value
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      String[] data = value.toString().split(",");
      context.write(new Text(data[5]), one); 
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
    Job job = Job.getInstance(conf, "Metin example");
    // icinde bulundugumuz class adi - yukarida goruntuleyebiliriz.
    job.setJarByClass(Metin.class);

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
