require 'java'
require 'jruby/core_ext'

java_import 'com.sensetime.utils.ProcToBytesService'

ProcToBytesService.new.basicLoad JRuby.runtime

module WordCount

  CONST1 = 'XXX'
  def self.main1
    # t1 = File.open('proc.bin', 'rb').read
    pr = Marshal.load(File.open('proc.bin', 'rb').read)
    p pr
    p pr.call('aaa bbb', 9)
    #a = Proc.from_bytes(bytes[0], bytes[1])
    #p a
    #p a.call('dd bb')

  end

  def self.main
    bytes = Marshal.load(File.open('proc.bin', 'rb').read)
    p bytes
    a = Proc.from_bytes(bytes[0], bytes[1])
    p a
    p a.call('dd bb')
  end


end

WordCount.main1
