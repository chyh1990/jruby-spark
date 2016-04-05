#!/bin/bash

JRUBY_SPARK_VERSION="1.0"

if [ -z $SPARK_JAR ]; then
	echo 'SPARK_JAR not set'
	exit 1
fi

if [ -z $JRUBY_HOME ]; then
	echo 'JRUBY_HOME not set'
	exit 1
fi

SELF_PATH=$(builtin cd -P -- "$(dirname -- "$0")" >/dev/null && pwd -P) && \
	SELF_PATH=$SELF_PATH/$(basename -- "$0")

SELF_DIR=$(dirname -- "$SELF_PATH")

if [ -z $JRUBY_SPARK_HOME ]; then
	JRUBY_SPARK_HOME=$(dirname -- "$SELF_DIR")
	# try
	for lib in $JRUBY_SPARK_HOME/lib $JRUBY_SPARK_HOME/build/libs; do
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
echo "using $jruby_spark_jar" >&2

export CLASSPATH=$jruby_spark_jar:$SPARK_JAR:$CLASSPATH

echo "Bootstrap JRuby-Spark..."
$JRUBY_HOME/bin/jruby -J-Djruby.ir.writing=true classpath:jruby_spark/repl.rb


