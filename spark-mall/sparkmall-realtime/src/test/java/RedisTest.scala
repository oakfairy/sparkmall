import com.atguigu.sparkmall0529.common.util.JedisUtil
import redis.clients.jedis.Jedis

object RedisTest {
  def main(args: Array[String]): Unit = {
    val jedis: Jedis = JedisUtil.getJedis
    val result: String = jedis.ping()
    println(result)
  }

}
