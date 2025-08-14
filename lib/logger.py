class Log4J:
    def __init__(self, spark):
        log4j = spark._jvm.org.apache.log4j
        root_class = "udemy.spark.examples"
        conf = spark.sparkContext.getConf()
        app_name = conf.get("spark.app.name") #.appName("Hello Spark") will return Hello Spark
        self.logger = log4j.LogManager.getLogger(root_class + "." + app_name)

    def warn(self, message): #will pass on message to logger's warning method
        self.logger.warn(message)

    def info(self, message):
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message)

    def debug(self, message):
        self.logger.debug(message)