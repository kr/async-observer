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

require 'async_observer/queue'

module AsyncObserver::Extensions
  def async_send(selector, *args)
    async_send_opts(selector, {}, *args)
  end

  def async_send_opts(selector, opts, *args)
    AsyncObserver::Queue.put_call!(self, selector, opts, args)
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

  attr_accessor :async_hooks

  def add_async_hook(hook, block)
    prepare_async_hook_list(hook) << block
  end

  def prepare_async_hook_list(hook)
    (self.async_hooks ||= {})[hook] ||= new_async_hook_list(hook)
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
    self.async_hooks[hook].each{|b| b.call(o)}
  end
end
