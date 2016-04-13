require 'jruby_spark/sql_schema'

module JRubySpark
  java_import 'org.apache.spark.sql.DataFrame'

  class SQLContext < Delegator
    def initialize sc
      ctx = Java::OrgApacheSparkSql::SQLContext.new sc.__getobj__
      @jctx = ctx
    end

    def create_data_frame data, schema
      data = data.__getobj__ if RDDLike === data
      raise ArgumentError, 'not a schema' unless schema.respond_to? :dataframe_schema
      @jctx.createDataFrame data, schema.dataframe_schema
    end

    def __getobj__
      @jctx
    end

    def inspect
      "#<#{self.class} jctx=#{@jctx.inspect}>"
    end
  end

  class DataFrame
    def rdd
      RDD.new self.javaRDD
    end
  end
end

