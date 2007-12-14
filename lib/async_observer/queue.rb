
require 'beanstalk-client'

module AsyncObserver; end

class AsyncObserver::Queue; end

class << AsyncObserver::Queue
  DEFAULT_PRI = 512
  attr_accessor :queue, :app_version

  # This is a fake worker instance for running jobs synchronously.
  def sync_worker()
    @sync_worker ||= AsyncObserver::Worker.new(binding)
  end

  # This runs jobs synchronously; it's used when no queue is configured.
  def sync_run(obj, pri=DEFAULT_PRI)
    body = YAML.dump(obj)
    job = Beanstalk::Job.new(AsyncObserver::FakeConn.new(), 0, pri, body)
    sync_worker.dispatch(job)
    sync_worker.do_all_work()
  end

  def put!(obj, pri=DEFAULT_PRI)
    return sync_run(obj, pri) if !queue
    queue.connect()
    queue.yput(obj, pri)
  end

  def put_call!(obj, sel, args=[])
    code = gen(obj, sel, args)
    put!(pkg(code), DEFAULT_PRI)
    RAILS_DEFAULT_LOGGER.info("put #{DEFAULT_PRI} #{code}")
  end

  def pkg(code)
    {
      :type => :rails,
      :code => code,
      :appver => AsyncObserver::Queue.app_version,
    }
  end

  # Be careful not to pass in a selector that's not valid ruby source.
  def gen(obj, selector, args)
    obj.rrepr + '.' + selector.to_s + '(' + gen_args(args) + ')'
  end

  def gen_args(args)
    args.rrepr[1...-1]
  end
end

class AsyncObserver::FakeConn
  def delete(x)
  end

  def release(x, y, z)
  end

  def bury(x, y)
  end

  def addr
    '<none>'
  end

  def job_stats(id)
    {
      'id' => id,
      'state' => 'reserved',
      'age' => 0,
      'delay' => 0,
      'time-left' => 5000,
      'timeouts' => 0,
      'releases' => 0,
      'buries' => 0,
      'kicks' => 0,
    }
  end
end

# This is commented out to workaround (what we think is) a ruby bug in method
# lookup. Somehow the methods defined here are being used instead of ones in
# ActiveRecord::Base.
#class Object
#  def rrepr()
#    raise ArgumentError.new('no consistent external repr for ' + self.inspect)
#  end
#end

class Symbol
  def rrepr() inspect end
end

class Module
  def rrepr() name end
end

class NilClass
  def rrepr() inspect end
end

class FalseClass
  def rrepr() inspect end
end

class TrueClass
  def rrepr() inspect end
end

class Numeric
  def rrepr() inspect end
end

class String
  def rrepr() inspect end
end

class Array
  def rrepr() '[' + map(&:rrepr).join(', ') + ']' end
end

class Hash
  def rrepr() '{' + map{|k,v| k.rrepr + '=>' + v.rrepr}.join(', ') + '}' end
end

module AsyncObserver::Extensions
  def rrepr() "#{self.class.rrepr}.find(#{id.rrepr})" end
end
