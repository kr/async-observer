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


begin
  require 'mysql'
rescue LoadError
  # Ignore case where we don't have mysql
end
require 'async_observer/queue'
require 'async_observer/util'

module AsyncObserver; end

class AsyncObserver::Worker
  extend AsyncObserver::Util
  include AsyncObserver::Util

  SLEEP_TIME = 60 if !defined?(SLEEP_TIME) # rails loads this file twice

  class << self
    attr_accessor :finish
    attr_accessor :custom_error_handler
    attr_accessor :before_filter
    attr_writer :handle

    def handle
      @handle or raise 'no custom handler is defined'
    end

    def error_handler(&block)
      self.custom_error_handler = block
    end

    def before_reserves
      @before_reserves ||= []
    end

    def before_reserve(&block)
      before_reserves << block
    end

    def run_before_reserve
      before_reserves.each {|b| b.call()}
    end
  end

  def initialize(top_binding)
    @top_binding = top_binding
    @stop = false
  end

  def main_loop()
    trap('TERM') { @stop = true }
    loop do
      break if @stop
      safe_dispatch(get_job())
    end
  end

  def startup()
    log_bracketed('worker-startup') do
      appver = AsyncObserver::Queue.app_version
      RAILS_DEFAULT_LOGGER.info "pid is #{$$}"
      RAILS_DEFAULT_LOGGER.info "app version is #{appver}"
      mark_db_socket_close_on_exec()
      if AsyncObserver::Queue.queue.nil?
        RAILS_DEFAULT_LOGGER.info 'no queue has been configured'
        exit(1)
      end
      AsyncObserver::Queue.queue.watch(appver) if appver
    end
    flush_logger
  end

  # This prevents us from leaking fds when we exec. Only works for mysql.
  def mark_db_socket_close_on_exec()
    ActiveRecord::Base.active_connections.each(&:set_close_on_exec)
  rescue NoMethodError
  end

  def shutdown()
    log_bracketed('worker-shutdown') do
      do_all_work()
    end
  end

  def run()
    startup()
    main_loop()
  rescue Interrupt
    shutdown()
  end

  def q_hint()
    @q_hint || AsyncObserver::Queue.queue
  end

  # This heuristic is to help prevent one queue from starving. The idea is that
  # if the connection returns a job right away, it probably has more available.
  # But if it takes time, then it's probably empty. So reuse the same
  # connection as long as it stays fast. Otherwise, have no preference.
  def reserve_and_set_hint()
    t1 = Time.now.utc
    return job = q_hint().reserve()
  ensure
    t2 = Time.now.utc
    @q_hint = if brief?(t1, t2) and job then job.conn else nil end
  end

  def brief?(t1, t2)
    ((t2 - t1) * 100).to_i.abs < 10
  end

  def get_job()
    log_bracketed('worker-get-job') do
      loop do
        begin
          AsyncObserver::Queue.queue.connect()
          self.class.run_before_reserve
          return reserve_and_set_hint()
        rescue Interrupt => ex
          raise ex
        rescue SignalException => ex
          raise ex
        rescue Beanstalk::DeadlineSoonError
          # Do nothing; immediately try again, giving the user a chance to
          # clean up in the before_reserve hook.
          RAILS_DEFAULT_LOGGER.info 'Job deadline soon; you should clean up.'
        rescue Exception => ex
          @q_hint = nil # in case there's something wrong with this conn
          RAILS_DEFAULT_LOGGER.info(
            "#{ex.class}: #{ex}\n" + ex.backtrace.join("\n"))
          RAILS_DEFAULT_LOGGER.info 'something is wrong. We failed to get a job.'
          RAILS_DEFAULT_LOGGER.info "sleeping for #{SLEEP_TIME}s..."
          sleep(SLEEP_TIME)
        end
      end
    end
  end

  def dispatch(job)
    ActiveRecord::Base.verify_active_connections!
    return run_ao_job(job) if async_observer_job?(job)
    return run_other(job)
  end

  def safe_dispatch(job)
    log_bracketed('worker-dispatch') do
      RAILS_DEFAULT_LOGGER.info "got #{job.inspect}:\n" + job.body
      log_bracketed('job-stats') do
        job.stats.each do |k,v|
          RAILS_DEFAULT_LOGGER.info "#{k}=#{v}"
        end
      end
      begin
        return dispatch(job)
      rescue Interrupt => ex
        begin job.release() rescue :ok end
        raise ex
      rescue Exception => ex
        handle_error(job, ex)
      ensure
        flush_logger
      end
    end
  end

  def flush_logger
    if defined?(RAILS_DEFAULT_LOGGER) &&
        RAILS_DEFAULT_LOGGER.respond_to?(:flush)
      RAILS_DEFAULT_LOGGER.flush
    end
  end

  def handle_error(job, ex)
    if self.class.custom_error_handler
      self.class.custom_error_handler.call(job, ex)
    else
      self.class.default_handle_error(job, ex)
    end
  end

  def self.default_handle_error(job, ex)
    RAILS_DEFAULT_LOGGER.info "Job failed: #{job.server}/#{job.id}"
    RAILS_DEFAULT_LOGGER.info("#{ex.class}: #{ex}\n" + ex.backtrace.join("\n"))
    job.decay()
  rescue Beanstalk::UnexpectedResponse
  end

  def run_ao_job(job)
    RAILS_DEFAULT_LOGGER.info 'running as async observer job'
    f = self.class.before_filter
    f.call(job) if f
    job.delete if job.ybody[:delete_first]
    run_code(job)
    job.delete()
  rescue ActiveRecord::RecordNotFound => ex
    if job.age > 60
      job.delete() # it's old; this error is most likely permanent
    else
      job.decay() # it could be replication delay so retry quietly
    end
  end

  def run_code(job)
    eval(job.ybody[:code], @top_binding, "(beanstalk job #{job.id})", 1)
  end

  def async_observer_job?(job)
    begin job.ybody[:type] == :rails rescue false end
  end

  def run_other(job)
    RAILS_DEFAULT_LOGGER.info 'trying custom handler'
    self.class.handle.call(job)
  end

  def do_all_work()
    RAILS_DEFAULT_LOGGER.info 'finishing all running jobs. interrupt again to kill them.'
    f = self.class.finish
    f.call() if f
  end
end

class ActiveRecord::ConnectionAdapters::MysqlAdapter
  def set_close_on_exec()
    @connection.set_close_on_exec()
  end
end

class Mysql
  def set_close_on_exec()
    if @net
      @net.set_close_on_exec()
    else
      # we are in the c mysql binding
      RAILS_DEFAULT_LOGGER.info "Warning: we are using the C mysql binding, can't set close-on-exec"
    end
  end
end

class Mysql::Net
  def set_close_on_exec()
    @sock.fcntl(Fcntl::F_SETFD, Fcntl::FD_CLOEXEC)
  end
end
