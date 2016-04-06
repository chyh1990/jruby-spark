require 'java'

module JRubySpark
  class Schema
    java_import org.apache.spark.sql.Row
    java_import org.apache.spark.sql.RowFactory
    java_import 'org.apache.spark.sql.types.DataTypes'
    java_import 'org.apache.spark.sql.types.StructType'
    java_import 'org.apache.spark.sql.types.StructField'

    def initialize
      raise 'never call me'
    end

    def self.build_struct
      _layout!

      fields = @@schema.map{|e|
        dt = case e[1].to_s.to_sym
        when :Fixnum
          DataTypes::IntegerType
        # XXX
        when :Integer
          DataTypes::LongType
        when :Float
          DataTypes::DoubleType
        when :String
          DataTypes::StringType
        when :Time
          DataTypes::DateType
        else
          raise ArgumentError, 'unknown datatype mapping'
        end
        DataTypes.createStructField(e[0].to_s, dt, e[2])
      }
      schema = DataTypes.createStructType(fields)
      schema
    end

    def self.dataframe_schema
      @@dataframe_schema ||= self.build_struct
    end

    def self._layout!
      @@schema ||= []
      @@schema_index ||= @@schema.to_enum.with_index.map{|e, i| [e[0], i]}.to_h
    end

    def self.field name, type, nullable = true
      @@schema ||= []
      @@schema << [name, type, nullable]
    end

    def self.fields
      @@schema.map{ |e| e.first}
    end

    # return Row
    def self.create *args
      _layout!
      raise ArgumentError unless args.size >= 1

      vals = nil
      if Hash === args.first
        vals = Array.new @@schema.size
        args[0].each {|k, v|
          idx = @@schema_index[k]
          raise "#{k} is not a field in #{self}" unless idx
          vals[idx] = v
        }
      else
        raise ArgumentError, 'field count mismatch' unless args.size == @@schema.size
        vals = args
      end
      # coercing handle by JRuby
      RowFactory.create *vals
    end
  end
end

# example
# class Person < JRubySpark::Schema
#   field :name, String
#   field :age, Integer
# end

