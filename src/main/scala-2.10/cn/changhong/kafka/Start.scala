package cn.changhong.kafka

import java.io.FileInputStream
import java.util.Properties
import java.util.concurrent.Executors

import kafka.consumer.{Consumer, ConsumerConfig}
import kafka.producer._
import kafka.utils.{VerifiableProperties, Logging}

import scala.util.Random


/**
 * Created by yangguo on 15-7-23.
 */
class CustomizePartitioner(props: VerifiableProperties = null) extends Partitioner{
  override def partition(key: Any, numPartitions: Int): Int = new DefaultPartitioner(props).partition(key,numPartitions)
}
object Start {
  private def createConfig(path:String)={
    val _props=new Properties()
    val inputStream=new FileInputStream(path)
    _props.load(inputStream)
    _props
  }


  object ConsumerExample extends Logging{
    def apply(config:Properties)={
      val topic=config.getProperty("topic.id","topic5")
      val consumerConfig=new ConsumerConfig(config)
      val consumer=Consumer.create(consumerConfig)
      val executor=Executors.newFixedThreadPool(4)
      val topicCountMap=Map(topic->4)
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
      (1 to 10).foreach{index=>
        val runtime=System.currentTimeMillis()
        val msg=s"msgId[$runtime],eventId[$index],msg[helloworld]"
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
    val config=createConfig(propsFile)
    if(executorType.equalsIgnoreCase("producer")){
      run(ProducerExample(config))
    }else if(executorType.equalsIgnoreCase("consumer")) {
      run(ConsumerExample(config))
    }else{
      throw new RuntimeException(s"Not Find Valid Type[$executorType],Please Enter Valid Type [producer or consumer]")
    }
  }

}
