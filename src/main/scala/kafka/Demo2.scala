package kafka

import org.slf4j.{Logger, LoggerFactory}

object Demo2 {


  private val logger: Logger = LoggerFactory.getLogger(Demo2.getClass)

  def main(args: Array[String]): Unit = {

    logger.info("logger info")
    logger.warn("logger warn")
    logger.error("logger error")
  }
}
