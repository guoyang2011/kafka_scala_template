package cn.changhong.kafka

import java.util.Properties

import cn.changhong.kafka.util.KafkaConfig
import kafka.admin.AdminUtils
import kafka.server.KafkaConfig
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.serialize.ZkSerializer

/**
 * Created by yangguo on 15-7-27.
 */
object TopicAdmin {
  def createZkClient(config:Properties=new Properties(),sessionTimeout:Int=10000,connTimeout:Int=10000,serializer:ZkSerializer=ZKStringSerializer) ={
    val zkHost=config.getProperty("zookeeper.connect","ch1:2181")
    new ZkClient(zkHost,sessionTimeout,connTimeout,serializer)
  }

  /**
   *
   * @param topicName
   * @param zkClient
   * @param config
   * @param partitioner
   * @param replicaFactor
   */
  def createTopic(topicName:String,zkClient: ZkClient,config:Properties,partitioner:Int,replicaFactor:Int): Unit ={
    AdminUtils.createTopic(zkClient,topicName,partitioner,replicaFactor,config)
  }
  def main(args: Array[String]) {
    require(args.length>=2,"Please Enter New Topic Name And KafkaConfig File Path!")
    val topicName=args(0)
    val configPath=args(1)
    val properties=KafkaConfig.loadFromPath(configPath)
    val zkClient=createZkClient()
    createTopic(topicName,zkClient,new Properties(),4,2)
  }
}
