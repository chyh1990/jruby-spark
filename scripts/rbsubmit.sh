#!/bin/bash

set -e

SELF_PATH=$(builtin cd -P -- "$(dirname -- "$0")" >/dev/null && pwd -P) && \
	SELF_PATH=$SELF_PATH/$(basename -- "$0")

SELF_DIR=$(dirname -- "$SELF_PATH")

source "$SELF_DIR/common.sh"

if [[ -z "$1" ]]; then
	echo "Usage: $0 [spark-submit options] --rb-files <ruby file | directory> options..."
	exit 1
fi

spark_args=()
# argument parser
while [[ $# > 1 ]]
do
	key="$1"
	case $key in
		--rb-files)
			rbmain_full=$2
			shift
			shift
			break
			;;
		*)
			spark_args+=("$key")
			shift
			;;
	esac
done

echo "spark-submit arguments: ${spark_args[@]}" >&2

if [[ -z "$rbmain_full" ]]; then
	echo "ruby file not specified" >&2
	exit 1
fi


tmpjar=$(mktemp /tmp/spark-submit-XXXXX.jar)

if [[ -f "$rbmain_full" ]]; then
	echo "packing up user scripts..." >&2
	rbmain=$(basename "$rbmain_full")

	# TMPDIR=`mktemp -d`
	rbmain_dir=$(dirname -- "$rbmain_full")

	if [[ -z "$rbmain_dir" ]]; then
		rbmain_dir="."
	fi

	jar cf $tmpjar -C "$rbmain_dir" "$rbmain"
elif [[ -d "$rbmain_full" ]]; then
	echo "packing up user script directory..." >&2
	dir=$(basename "$rbmain_full")
	rbmain="$dir".rb

	jar cf $tmpjar -C "$rbmain_full" .
else
	echo "rb file or directory not found"
	exit 1
fi

if [ -z $SPARK_HOME ]; then
	echo 'SPARK_HOME not set'
	exit 1
fi

jars=$JRUBY_SPARK_HOME/libs/jruby-complete-"$JRUBY_VERSION".jar,"$tmpjar"

bootstrap_file="classpath:$rbmain"

echo "submitting jobs..." >&2

$SPARK_HOME/bin/spark-submit  \
	--conf 'spark.driver.extraJavaOptions=-Djruby.ir.writing=true' \
	--conf "spark.executor.jruby.bootstrap_file=$bootstrap_file" \
	--jars $jars \
	--class org.apache.spark.jruby.ScriptMain \
	"${spark_args[@]}" \
	"$jruby_spark_jar" "$bootstrap_file" "$@"

rm -f "$tmpjar"

