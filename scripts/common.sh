JRUBY_SPARK_VERSION="1.0"
JRUBY_VERSION="9.1.0.0-SNAPSHOT"

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


