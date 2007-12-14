require 'async_observer/queue'

module AsyncObserver::Extensions
  def async_send(selector, *args)
    AsyncObserver::Queue.put_call!(self, selector, args)
  end
end
[Symbol, Module, Numeric, String, Array, Hash, ActiveRecord::Base].each do |c|
  c.send :include, AsyncObserver::Extensions
end

HOOKS = [:after_create, :after_update, :after_save]

class << ActiveRecord::Base
  HOOKS.each do |hook|
    code = %Q{def async_#{hook}(&b) add_async_hook(#{hook.inspect}, b) end}
    class_eval(code, "generated code from #{__FILE__}:#{__LINE__ - 1}", 1)
  end

  def add_async_hook(hook, block)
    prepare_async_hook_list(hook) << block
  end

  def prepare_async_hook_list(hook)
    (@async_hooks ||= {})[hook] ||= new_async_hook_list(hook)
  end

  def new_async_hook_list(hook)
    ahook = :"_async_#{hook}"

    # This is for the producer's benefit
    send(hook){|o| o.async_send(ahook)}

    # This is for the worker's benefit
    define_method(ahook) do
      self.class.run_async_hooks(hook, self)
    end

    return []
  end

  def run_async_hooks(hook, o)
    @async_hooks[hook].each{|b| b.call(o)}
  end
end
