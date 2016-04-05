require 'logger'
require 'net/http'
require 'uri'
require 'tempfile'
require 'digest'

require 'jruby_spark/jruby_spark'

raise 'repl_executor.rb run in spark executor only' if JRubySpark.main?

class SparkREPLLoader
  @@logger ||= Logger.new $stderr
  @@last_md5 = nil

  def self.info str
    @@logger.info('JRubySparkExecutor') { str }
  end

  def self.error str
    @@logger.error('JRubySparkExecutor') { str }
  end

  self.info 'JRuby Spark executor started'

  def self.load_code path
    md5 = Digest::MD5.file(path).hexdigest
    return if @@last_md5 == md5
    @@last_md5 = md5

    SparkREPLLoader.info 'Reloading executor repl main'

    begin
      Object.__send__(:remove_const, :REPLTopLevel)
    rescue
      SparkREPLLoader.info 'REPLTopLevel not exist'
    end

    SparkREPLLoader.info 'Reloading done repl main'

    # p File.read(f.path)
    load path
  end

  def self.reload_user_code
    begin
      Tempfile.open(['spark-repl-', '.rb']) do |f|
        resp = Net::HTTP.get_response(URI('http://localhost:61637' + '/_repl_main.rb'))
        raise "Failed to download code: #{resp.code}" unless resp.code == '200'

        f.write resp.body
        f.close
        SparkREPLLoader.load_code f.path
        f.unlink
      end
    rescue Exception => e
      SparkREPLLoader.error "Fail to load repl main: #{e.message}"
      $stderr.puts e.backtrace.join("\n")
    end
  end
end