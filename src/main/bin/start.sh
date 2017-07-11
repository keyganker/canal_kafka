#!/bin/bash 

current_path=`pwd`
case "`uname`" in
    Linux)
                bin_abs_path=$(readlink -f $(dirname $0))
                ;;
        *)
                bin_abs_path=`cd $(dirname $0); pwd`
                ;;
esac

base=$(dirname $bin_abs_path)
kafka_conf=$base/conf/kafkaConfig.properties
canal_conf=$base/conf/canalConfig.properties
export LANG=en_US.UTF-8
export BASE=$base
if [ ! -d $base/logs ] ; then
        mkdir -p $base/logs
fi
## set java path
if [ -z "$JAVA" ] ; then
  JAVA=$(which java)
fi

JAVA_OPTS="-Xms512m -Xmx1024m -XX:NewSize=384m -XX:MaxNewSize=450m -XX:PermSize=128m -XX:MaxPermSize=1024m -XX:+CMSParallelRemarkEnabled -XX:+UseCMSCompactAtFullCollection -XX:+UseFastAccessorMethods -XX:+HeapDumpOnOutOfMemoryError"
CANAL_OPTS="-DcanalConfig=$canal_conf"
KAFKA_OPTS="-DkafkaConfig=$kafka_conf"
LOGBACK_OPTS="-Dlogback.configurationFile=$base/conf/logback.xml"

for i in $base/lib/*;
        do CLASSPATH=$i:"$CLASSPATH";
done
CLASSPATH="$base/conf:$CLASSPATH";

echo "canalConfig: " $CANAL_OPTS;
echo "canalConfig: " $KAFKA_OPTS;
echo "classpath: " $CLASSPATH;

# start cluster canal client 
$JAVA -server $JAVA_OPTS $CANAL_OPTS $KAFKA_OPTS $LOGBACK_OPTS -cp $CLASSPATH com.yijie.kafka.producer.ClusterCanalClient 1>>$base/logs/canalClient.log 2>&1 &

# start canal client
# $JAVA -server $CANAL_OPTS $KAFKA_OPTS $LOGBACK_OPTS -cp $CLASSPATH com.yijie.kafka.producer.CanalClient 1>>$base/logs/canalClient.log 2>&1 &