package cn.changhong.kafka

import java.util.Properties
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

import cn.changhong.kafka.util.KafkaConfig
import kafka.consumer.{Consumer, ConsumerConfig}
import kafka.producer._
import kafka.serializer.{Encoder, StringEncoder}
import kafka.utils.{Logging, VerifiableProperties}


import net.liftweb.json.Extraction._
import net.liftweb.json._

import scala.util.Random


/**
 * Created by yangguo on 15-7-23.
 */
class CustomizePartitioner(props: VerifiableProperties = null) extends Partitioner{
  override def partition(key: Any, numPartitions: Int): Int = new DefaultPartitioner(props).partition(key,numPartitions)
}
class ObjToJsonStringEncoder(props: VerifiableProperties = null) extends Encoder[Any]{
  implicit lazy val jsonFormat=DefaultFormats
  override def toBytes(t: Any): Array[Byte] = {
    val jsonString=compact(render(decompose(t)))
    println(jsonString)
    new StringEncoder(props).toBytes(jsonString)
  }
}


object Start {
  object ConsumerExample extends Logging{
    def apply(config:Properties)={
      val topic=config.getProperty("topic.id","topic5")
      val consumerConfig=new ConsumerConfig(config)
      val consumer=Consumer.create(consumerConfig)
      val executor=Executors.newFixedThreadPool(4)
      /**
       * setting how many threads for each topic,top:each thread for each topic partitioner is best
       * if you provide more threads than there are partitions on the topic, some threads will never see a message
       * if you have more partitions than you have threads, some threads will receive data from multiple partitions
       * if you have multiple partitions per thread there is NO guarantee about the order you receive messages, other than that within the partition the offsets will be sequential. For example, you may receive 5 messages from partition 10 and 6 from partition 11, then 5 more from partition 10 followed by 5 more from partition 10 even if partition 11 has data available.
       * adding more processes/threads will cause Kafka to re-balance, possibly changing the assignment of a Partition to a Thread.
       * some detail see this page:https://cwiki.apache.org/confluence/display/KAFKA/Consumer+Group+Example
       * */
      val topicCountMap=Map(topic->1)

      val consumerMap=consumer.createMessageStreams(topicCountMap)
      consumerMap.get(topic) match {
        case Some(streams) =>
          streams.foreach { stream =>
            val busTask = new Runnable {
              override def run(): Unit = {
                val it = stream.iterator()
                Iterator continually (it.hasNext()) takeWhile (_ == true) foreach { bool =>
                  val msg = new String(it.next().message(), "utf8")
                  println(s"groupId[${config.getProperty("group.id","group1")}],${System.currentTimeMillis()},Thread ${Thread.currentThread().getId},msg=$msg")
                }
                println("isOver")
              }
            }
            executor.submit(busTask)
          }
        case None=>throw new RuntimeException("No Valid Kafka Stream!")
      }
    }
  }
  object ProducerExample extends Logging{
    val rnd=new Random()
    def apply(config:Properties)={
      val _config=new ProducerConfig(config)
      val producer=new Producer[String,String](_config)
      val index=new AtomicInteger(0)
      while(true){
        Thread.sleep(100)
        val runtime=System.currentTimeMillis()
        val msg=s"msgId[$runtime],eventId[${index.getAndIncrement()}],msg[helloworld]"
        val data=new KeyedMessage[String,String](config.getProperty("topic.id","topic5"),rnd.nextInt().toString,msg)
        producer.send(data)

      }
    }
  }
  def main(args: Array[String]) {
    require(args.length>=2,"Please Enter executor type[producer or consumer] and Kafka Config File Path")
    def run(func: =>Unit)={
      new Thread(new Runnable {
        override def run(): Unit = func
      }).start()
    }
    val executorType=args(0)
    val propsFile=args(1)
    val config=KafkaConfig.loadFromPath(propsFile)
    if(executorType.equalsIgnoreCase("producer")){
      run(ProducerExample(config))
    }else if(executorType.equalsIgnoreCase("consumer")) {
      run(ConsumerExample(config))
    }else{
      throw new RuntimeException(s"Not Find Valid Type[$executorType],Please Enter Valid Type [producer or consumer]")
    }

  }
}
