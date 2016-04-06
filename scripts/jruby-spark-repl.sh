#!/bin/bash

JRUBY_SPARK_VERSION="1.0"
JRUBY_VERSION="9.1.0.0-SNAPSHOT"

SELF_PATH=$(builtin cd -P -- "$(dirname -- "$0")" >/dev/null && pwd -P) && \
	SELF_PATH=$SELF_PATH/$(basename -- "$0")

SELF_DIR=$(dirname -- "$SELF_PATH")

if [ -z $JRUBY_SPARK_HOME ]; then
	JRUBY_SPARK_HOME=$(dirname -- "$SELF_DIR")
	# try
	for lib in $JRUBY_SPARK_HOME/libs $JRUBY_SPARK_HOME/build/libs; do
		if [ -n "$jruby_spark_jar" ]; then
			break
		fi
		for name in jruby-spark-"$JRUBY_SPARK_VERSION".jar \
			jruby-spark-"$JRUBY_SPARK_VERSION"-SNAPSHOT.jar; do
			if [ -f "$lib/$name" ]; then
				jruby_spark_jar="$lib/$name"
				break
			fi
		done
	done
else
	jruby_spark_jar=$(ls $JRUBY_SPARK_HOME/lib/jruby-spark-*.jar | head -n1)
fi

if [ -z "$jruby_spark_jar" ]; then
	echo "JRuby Spark jar not found"
	exit 1
fi

echo "using $jruby_spark_jar" >&2

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


