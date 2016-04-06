# JRuby on Spark

Yuheng Chen <yuhengchen@sensetime.com>

Combining Spark with elegant Ruby closure on JVM.

## Setting up

Download Spark 1.6 distribution.

Note that if you use `rvm` or similiar tools, use

```
rvm use system
```
to avoid loading installed gems.

## REPL

Run JRuby Spark REPL shell:

```
export SPARK_HOME=$YOUR_SPARK_HOME
./bin/jruby-spark-repl.sh
```

By default, `jruby-spark-repl.sh` use `spark-submit` to submit the REPL job
locally, similiar to `pyspark`.

Run the word count example:

```
$sc.text_file('SOME_TXT_FILE.txt').flat_map{|x| x.downcase.split(/[^\w']+/)}
	.map_to_pair{|x| tuple(x, 1)}.reduce_by_key(:+).collect_as_map
```

Note how well Ruby fits into Spark framework:

* Block and lambda
* symbol to `proc`
* use powerful utilities in Ruby's standard library

## One script job

## Job with depedencies

## Architecture

### Closure marhshalling

### Java interoperation

### Size estimator

### Conclusion

