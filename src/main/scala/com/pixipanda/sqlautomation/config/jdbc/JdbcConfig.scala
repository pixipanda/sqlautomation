package com.pixipanda.sqlautomation.config.jdbc

import java.util.Properties

import com.pixipanda.sqlautomation.config.ConfigRegistry
import com.typesafe.config.Config

import scala.collection.JavaConverters._
import scala.collection.mutable

case class JdbcConfig(name:String,
                      driver:String,
                      url:String,
                      username: String,
                      password: String,
                      properties:Properties)

object JdbcConfig {



  lazy val jdbcConfigs: Map[String, JdbcConfig] = parseJdbc(ConfigRegistry.config)


  def parseJdbc(config: Config): Map[String, JdbcConfig] = {

    val props = new Properties()
    config.getConfigList("jdbc-connections").asScala.map(config => {
      props.clear()
      val name = config.getString("name")
      val driver = config.getString("driver")
      val url = config.getString("url")
      val username = config.getString("username")
      val password = config.getString("password")
      /*val batchSize = config.getInt("batchSize")
      val p =  config.getConfig("properties")
      if(!p.isEmpty) {
        p.entrySet().asScala.foreach(e => {
          val key = e.getKey
          val value = e.getValue.unwrapped().toString
          props.setProperty(key, value)
        })
      }*/
      (name, JdbcConfig(name, driver, url, username, password, props))

    }).toMap
  }


  def getJdbcAsMap(name: String): Map[String, String] = {

    val options = mutable.Map[String, String]()

    val jdbcConfigOption = jdbcConfigs.get(name)
    jdbcConfigOption match {
      case Some(jdbcConfig) =>
        options.put("driver", jdbcConfig.driver)
        options.put("url", jdbcConfig.url)
        options.put("username", jdbcConfig.username)
        options.put("password", jdbcConfig.password)
        options.toMap
      case None => options.toMap
    }
  }
}