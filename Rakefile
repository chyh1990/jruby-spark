require 'rake/testtask'
require 'fileutils'

def version
  v = nil
  IO.foreach('build.gradle') do |l|
    if l.start_with? 'version'
      v = l.strip.split[1].gsub("'", '')
      break
    end
  end
  v
end

Rake::TestTask.new do |t|
  t.libs << "test"
  t.test_files = FileList['test/test*.rb']
  t.verbose = true
end

JAR_FILE="build/libs/jruby-spark-#{version}.jar"
file JAR_FILE do |t|
  sh "./gradlew jar --info"
end

task :package => JAR_FILE do |t|
  v = version
  puts "VERSION: #{v}"
  Rake::Task[JAR_FILE].execute
  tag = `git describe --always --dirty`.strip
  out = "pkg/jruby-spark-#{tag}"

  bindir = File.join(out, 'bin')
  libsdir = File.join(out, 'libs')
  mkdir_p [bindir, libsdir]
  cp Dir.glob('libs/jruby-complete-*.jar'), libsdir
  cp Dir.glob('libs/*gems.jar'), libsdir
  cp JAR_FILE, libsdir
  ['jruby-spark-repl.sh', 'rbsubmit.sh', 'common.sh'].each do |f|
    cp File.join('scripts', f), bindir
  end

  cp 'README.md', out
  cp 'examples', out

  outpkg = out + ".tar.gz"
  puts "Packing #{outpkg}"
  sh "cd pkg && tar cfz #{File.basename outpkg} #{File.basename out}"
end

