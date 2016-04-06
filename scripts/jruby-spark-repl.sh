#!/bin/bash

SELF_PATH=$(builtin cd -P -- "$(dirname -- "$0")" >/dev/null && pwd -P) && \
	SELF_PATH=$SELF_PATH/$(basename -- "$0")

SELF_DIR=$(dirname -- "$SELF_PATH")

source "$SELF_DIR/common.sh"
# jruby_spark_lib_dir=$(dirname -- "$jruby_spark_jar")

echo "Bootstrap JRuby-Spark..."

if [[ "$1" == "--standalone" ]]; then
	if [ -z $SPARK_JAR ]; then
		echo 'SPARK_JAR not set'
		exit 1
	fi

	if [ -z $JRUBY_HOME ]; then
		echo 'JRUBY_HOME not set'
		exit 1
	fi

	export CLASSPATH="$jruby_spark_jar:$SPARK_JAR:$CLASSPATH"

	$JRUBY_HOME/bin/jruby -J-Djruby.ir.writing=true classpath:jruby_spark/repl.rb
else
	if [ -z $SPARK_HOME ]; then
		echo 'SPARK_HOME not set'
		exit 1
	fi
	jars=$JRUBY_SPARK_HOME/libs/jruby-complete-"$JRUBY_VERSION".jar,$JRUBY_SPARK_HOME/libs/pry-gems.jar
	$SPARK_HOME/bin/spark-submit --master=local --conf \
		'spark.driver.extraJavaOptions=-Djruby.ir.writing=true' \
		--jars $jars \
		"$jruby_spark_jar" org.apache.spark.jruby.REPLMain
fi


