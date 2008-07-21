# async-observer - Rails plugin for asynchronous job execution

# Copyright (C) 2007 Philotic Inc.

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


require 'beanstalk-client'

module AsyncObserver; end

class AsyncObserver::Queue; end

class << AsyncObserver::Queue
  DEFAULT_PRI = 512
  DEFAULT_FUZZ = 0
  DEFAULT_DELAY = 0
  DEFAULT_TTR = 120
  DEFAULT_TUBE = 'default'

  attr_accessor :queue, :app_version, :after_put

  # This is a fake worker instance for running jobs synchronously.
  def sync_worker()
    @sync_worker ||= AsyncObserver::Worker.new(binding)
  end

  # This runs jobs synchronously; it's used when no queue is configured.
  def sync_run(obj)
    body = YAML.dump(obj)
    job = Beanstalk::Job.new(AsyncObserver::FakeConn.new(), 0, body)
    sync_worker.dispatch(job)
    sync_worker.do_all_work()
    return 0, '0.0.0.0'
  end

  def put!(obj, pri=DEFAULT_PRI, delay=DEFAULT_DELAY, ttr=DEFAULT_TTR,
           tube=DEFAULT_TUBE)
    return sync_run(obj) if (:direct.equal?(pri) or !queue)
    queue.connect()
    queue.use(tube)
    info = [queue.yput(obj, pri, delay, ttr), queue.last_server]
    f = AsyncObserver::Queue.after_put
    f.call(*info) if f
    return info
  end

  SUBMIT_OPTS = [:pri, :fuzz, :delay, :ttr, :tube]

  def put_call!(obj, sel, opts, args=[])
    pri = opts.fetch(:pri, DEFAULT_PRI)
    fuzz = opts.fetch(:fuzz, DEFAULT_FUZZ)
    delay = opts.fetch(:delay, DEFAULT_DELAY)
    ttr = opts.fetch(:ttr, DEFAULT_TTR)
    tube = opts.fetch(:tube, (app_version or DEFAULT_TUBE))
    worker_opts = opts.reject{|k,v| SUBMIT_OPTS.include?(k)}

    pri = pri + rand(fuzz + 1) if !:direct.equal?(pri)

    code = gen(obj, sel, args)
    RAILS_DEFAULT_LOGGER.info("put #{pri} #{code}")
    put!(pkg(code, worker_opts), pri, delay, ttr, tube)
  end

  def pkg(code, opts)
    opts.merge(:type => :rails, :code => code)
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

class Range
  def rrepr() "(#{first.rrepr}#{exclude_end? ? '...' : '..'}#{last.rrepr})" end
end

class Time
  def rrepr() "Time.parse('#{self.inspect}')" end
end

module AsyncObserver::Extensions
  def rrepr()
    method = (respond_to? :get_cache) ? 'get_cache' : 'find'
    "#{self.class.rrepr}.#{method}(#{id.rrepr})"
  end
end
