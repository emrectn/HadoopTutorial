import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MapReduceSpread {

    public static class CustomCrimeTuple implements Writable {
        private String crime = new String("A");
        private long count = 1;
        
        public String getCrime() {
        return crime;
        }
        public void setCrime(String crime) {
        this.crime = crime;
        }
        public long getCount() {
        return count;
        }
        public void setCount(long count) {
        this.count = count;
        }
        public void readFields(DataInput in) throws IOException {
            crime = in.readUTF();
            count = in.readLong();
        }
        public void write(DataOutput out) throws IOException {
        out.writeUTF(crime);
        out.writeLong(count);
        }    
        public String toString() {
        return "-" + getCrime() + "\t" + getCount();
        }
    }

//veri,output-key, output-value
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, CustomCrimeTuple>{
    
    private CustomCrimeTuple crimeTuple = new CustomCrimeTuple();
    private Text crimeYear = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

        String[] data = value.toString().split(",");
        crimeTuple.setCrime(data[5]);
        crimeTuple.setCount(1);
        crimeYear.set(data[17]);
        context.write(crimeYear, crimeTuple);

    }
  }
//yukaridan alinan veriler
  public static class IntSumReducer
       extends Reducer<Text,CustomCrimeTuple,Text,CustomCrimeTuple> {
    private CustomCrimeTuple result = new CustomCrimeTuple();
    private Map<String, Integer> map;
    
    public void reduce(Text key, Iterable<CustomCrimeTuple> values,
    Context context
    ) throws IOException, InterruptedException {
        map = new HashMap<>();
        for (CustomCrimeTuple customCrimeTuple : values) {
            if(map.containsKey(customCrimeTuple.getCrime())) {
                Integer count = map.get(customCrimeTuple.getCrime());
                count = count + 1;
                map.put(customCrimeTuple.getCrime(), count);
            } else {
                map.put(customCrimeTuple.getCrime(), 1);
            }
        }
        for (String s: map.keySet()) {
            result.setCrime(s);
            result.setCount(map.get(s));
            context.write(key, result);
        }
    }
  }

  public static void main(String[] args) throws Exception {
    // Hadoop Configration
    Configuration conf = new Configuration();
    // Job'a isim veriyoruz- Monitor ederken web arayuzden takip edecegimiz isim
    Job job = Job.getInstance(conf, "Spread For Year Function");
    // icinde bulundugumuz class adi - yukarida goruntuleyebiliriz.
    job.setJarByClass(MapReduceSpread.class);

    // uc asamadan olusur bunlar. Mapper, Combiner, Reducer
    // Yukarida belirtilen classlari asagida setediyoruz
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);

    // Sonuc olarak emre:30 - key,value. Output key olarak bir text ve Output value olarak bir Int cikti uretmektedir.
    //Bunlar set edilir.
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(CustomCrimeTuple.class);

    // Kelimelerini sayacagimiz input dosyasi
    FileInputFormat.addInputPath(job, new Path(args[0]));
    // Sonuclari yazacagimiz dosya
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    // Thread mantiginda calistigi icin threadlerin olmesini bekliyor
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
