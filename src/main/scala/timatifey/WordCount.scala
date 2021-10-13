package timatifey

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.util.{Tool, ToolRunner}

import java.lang

object WordCount extends Configured with Tool {

  private val INPUT_PATH = "word_count.in"
  private val OUTPUT_PATH = "word_count.out"

  def main(args: Array[String]): Unit = {
    val codeResult: Int = ToolRunner.run(new Configuration(), this, args)
    System.exit(codeResult)
  }

  override def run(args: Array[String]): Int = {
    val wordCountJob = Job.getInstance(getConf, "Word count")
    wordCountJob.setJarByClass(getClass)

    wordCountJob.setOutputKeyClass(classOf[Text])
    wordCountJob.setOutputValueClass(classOf[IntWritable])

    wordCountJob.setMapperClass(classOf[WordCountMapper])
    wordCountJob.setReducerClass(classOf[WordCountReducer])
    wordCountJob.setNumReduceTasks(2)

    val in = new Path(getConf.get(INPUT_PATH))
    val out = new Path(getConf.get(OUTPUT_PATH))

    FileInputFormat.setInputPaths(wordCountJob, in)
    FileOutputFormat.setOutputPath(wordCountJob, out)

    val fs = FileSystem.get(getConf)
    if (fs.exists(out)) {
      fs.delete(out, true)
    }

    if (wordCountJob.waitForCompletion(true)) 0 else 1

  }
}

private class WordCountMapper extends Mapper[Object, Text, Text, IntWritable] {

  private val text = new Text()
  private val one = new IntWritable(1)

  override def map(key: Object,
                   value: Text,
                   context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
    val words = value.toString.toLowerCase.split("\\s")
    words.foreach(word => {
      if (!StringUtils.isEmpty(word)) {
        text.set(toValidForm(word))
        context.write(text, one)
      }
    })
  }

  private def toValidForm(value: String): String = {
    val sb = new StringBuilder()
    value
      .filter(ch => Character.isAlphabetic(ch) || Character.isDigit(ch))
      .map(ch => Character.toLowerCase(ch))
      .foreach(ch => sb.append(ch))

    sb.toString()
  }

}

private class WordCountReducer extends Reducer[Text, IntWritable, Text, IntWritable] {

  private val resultSum = new IntWritable(0)
  private var sum = 0

  override def reduce(key: Text,
                      values: lang.Iterable[IntWritable],
                      context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {

    sum = 0
    values.forEach(currentValue => sum += currentValue.get())

    resultSum.set(sum)
    context.write(key, resultSum)
  }
}