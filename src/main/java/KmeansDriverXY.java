import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class KmeansDriverXY {
    public static void main(String[] args) throws Exception 
    {
        List<String> chaine = new ArrayList<>();
        Configuration cf = new Configuration();
        System.setProperty("HADOOP_USER_NAME", "root");
        cf.set("fs.defaultFS","hdfs://localhost:9000");
        cf.set("dfs.replication","1");

        FileSystem fs = FileSystem.get(cf);
        Path path3 = new Path("/kmeans/resultat");

        int number = 0;
        boolean vrai = true;
        while (vrai==true)
        {
            if(fs.exists(path3))
                fs.delete(path3, true);

            Job job = Job.getInstance(cf,"Job Kmeans XY");
            job.setJarByClass(KmeansDriverXY.class);
            job.setMapperClass(KmeansMapperXY.class);
            job.setReducerClass(KmeansReducerXY.class);

            job.setMapOutputKeyClass(DoubleWritable.class);
            job.setMapOutputValueClass(DoubleWritable.class);
            job.setOutputKeyClass(DoubleWritable.class);
            job.setMapOutputValueClass(DoubleWritable.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.addCacheFile(new URI("hdfs://localhost:9000/kmeans/centers.txt"));
            FileInputFormat.addInputPath(job, new Path("/kmeans/points.txt"));
            FileOutputFormat.setOutputPath(job, path3);
            job.waitForCompletion(true);
            
            try {
                Path path1 = new Path("/kmeans/resultat/part-r-00000");
                System.out.println("Le contenu de fichier : ");
                FSDataInputStream fsi = fs.open(path1);
                BufferedReader br = new BufferedReader(new InputStreamReader(fsi));
                String ligne = null;
                while ((ligne = br.readLine())!=null)
                {
                    chaine.add(ligne);
                }
                System.out.println("==============================================");
                System.out.println(chaine.get(0));
                System.out.println(chaine.get(1));
                System.out.println(chaine.get(2));
                System.out.println("==============================================");
                br.close();
                fs.delete(path3, true);

            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
            if(chaine.get(0).split("\t")[0]==(chaine.get(0).split("\t")[1])
                    && chaine.get(1).split("\t")[0]==(chaine.get(1).split("\t")[1])
                    && chaine.get(2).split("\t")[0]==(chaine.get(2).split("\t")[1]))
            {
                System.out.println("***********************************************");
                System.out.println("***********************************************");
                System.out.println("***********************************************");
                System.out.println("***********************************************");
                vrai = false;
            }
            else
            {
                Path path2 = new Path("/kmeans/centers.txt");
                FSDataOutputStream fsdo = fs.create(path2);
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fsdo));
                String ligne = null;
                for(int i=0;i<3;i++)
                    bw.write(chaine.get(i).split("\t")[1]+"\n");
                bw.close();
            }

        }
    }

        /*
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"Job Kmeans XY");
        job.setJarByClass(KmeansDriverXY.class);
        job.setMapperClass(KmeansMapperXY.class);
        job.setReducerClass(KmeansReducerXY.class);

        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.addCacheFile(new URI("hdfs://localhost:9000/kmeans/centers.txt"));
        FileInputFormat.addInputPath(job, new Path("/kmeans/points.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/kmeans/resultat"));

        job.waitForCompletion(true);
*/
    }
