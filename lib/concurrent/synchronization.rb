require 'concurrent/utility/engine'
require 'concurrent/synchronization/abstract_object'
require 'concurrent/native_extensions' # JavaObject
require 'concurrent/synchronization/mutex_object'
require 'concurrent/synchronization/monitor_object'
require 'concurrent/synchronization/rbx_object'

module Concurrent
  module Synchronization
    class Object < case
                           when Concurrent.on_jruby?
                             JavaObject

                           when Concurrent.on_cruby? && (RUBY_VERSION.split('.').map(&:to_i) <=> [1, 9, 3]) <= 0
                             MonitorObject

                           when Concurrent.on_cruby?
                             MutexObject

                           when Concurrent.on_rbx?
                             RbxObject

                           else
                             MutexObject
                           end
    end
  end
end
