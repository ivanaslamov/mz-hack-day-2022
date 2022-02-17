package org.apache.flink.flight

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}

import java.util.Properties

object Application {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironment()

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "redpanda:9092")
    properties.setProperty("group.id", "flink")

    val kafkaConsumer = new FlinkKafkaConsumer[String](
      "materialized_rp_flight_information-u16-1645120717-13411117415973148608",
      new SimpleStringSchema(),
      properties
    )

    val stream: DataStream[String] = env.addSource(kafkaConsumer)

    stream.addSink(new LocalTunnelHttpSink())

    env.execute()
  }

  class LocalTunnelHttpSink extends RichSinkFunction[String] {
    var httpClient: CloseableHttpClient = null

    override def open(parameters: Configuration): Unit = {
      httpClient = HttpClients.createDefault()
    }

    override def close(): Unit = {
      httpClient.close()
    }

    override def invoke(value: String): Unit = {
      val httpPost = new HttpPost("https://f799-73-93-249-165.ngrok.io")
      httpPost.setEntity(new StringEntity(value))

      try {
        val response = httpClient.execute(httpPost)
        try {
          val httpStatusCode = response.getStatusLine.getStatusCode
          print(httpStatusCode)
        } finally if (response != null) response.close()
      }
    }
  }
}
