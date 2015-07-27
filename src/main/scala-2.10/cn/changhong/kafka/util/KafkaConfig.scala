package cn.changhong.kafka.util

import java.io.FileInputStream
import java.util.Properties

/**
 * Created by yangguo on 15-7-27.
 */
object KafkaConfig {
  kafka.log.LogConfig
  def loadFromPath(path:String)={
    val _props=new Properties()
    val inputStream=new FileInputStream(path)
    _props.load(inputStream)
    _props
  }
}
