package org.apache.spark.rpc

import org.apache.spark.{SecurityManager, SparkConf, SparkEnv}
import org.apache.spark.rpc.netty.NettyRpcEnvFactory
import org.scalatest.concurrent.Eventually.{eventually, interval, timeout}

import scala.concurrent.duration._

/**
 * Created by dongkelun on 2020/12/24 11:20
 */
object RpcTestMain {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val env = createRpcEnv(conf, "local", 0)
    @volatile var message: String = null
    val rpcEndpointRef = env.setupEndpoint("send-locally", new RpcEndpoint {
      override val rpcEnv = env

      override def onStart(): Unit = {
        println("start hello endpoint")
      }

      override def receive = {
        case msg: String => {
          println(msg)
          message = msg
        }
      }
    })
    rpcEndpointRef.send("hello")

    eventually(timeout(5.seconds), interval(10.milliseconds)) {
      assert("hello" == message)
    }

    if (env != null) {
      env.shutdown()
    }
    SparkEnv.set(null)

  }

  def createRpcEnv(
                    conf: SparkConf,
                    name: String,
                    port: Int,
                    clientMode: Boolean = false): RpcEnv = {
    val config = RpcEnvConfig(conf, "test", "localhost", "localhost", port,
      new SecurityManager(conf), 0, clientMode)
    new NettyRpcEnvFactory().create(config)
  }

}
