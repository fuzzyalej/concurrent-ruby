require 'concurrent/executor/executor'
require 'concurrent/logging'
require 'concurrent/synchronization'
require 'concurrent/atomic/event'

module Concurrent

  # @!macro [attach] executor_service
  # 
  #   An class that provides methods for controlling the lifecycle of an executor.
  class RubyExecutorService < Synchronization::Object
    include Executor
    include Logging

    # The set of possible fallback policies that may be set at thread pool creation.
    FALLBACK_POLICIES = [:abort, :discard, :caller_runs]

    def initialize
      super()
      @stop_event    = Event.new
      @stopped_event = Event.new
    end

    # @!macro [attach] executor_method_post
    #
    #   Submit a task to the executor for asynchronous processing.
    #
    #   @param [Array] args zero or more arguments to be passed to the task
    #
    #   @yield the asynchronous task to perform
    #
    #   @return [Boolean] `true` if the task is queued, `false` if the executor
    #     is not running
    #
    #   @raise [ArgumentError] if no task is given
    def post(*args, &task)
      raise ArgumentError.new('no block given') unless block_given?
      synchronize do
        # If the executor is shut down, reject this task
        return handle_fallback(*args, &task) unless running?
        execute(*args, &task)
        true
      end
    end

    # @!macro [attach] executor_method_left_shift
    #
    #   Submit a task to the executor for asynchronous processing.
    #
    #   @param [Proc] task the asynchronous task to perform
    #
    #   @return [self] returns itself
    def <<(task)
      post(&task)
      self
    end

    # @!macro [attach] executor_method_running_question
    #
    #   Is the executor running?
    #
    #   @return [Boolean] `true` when running, `false` when shutting down or shutdown
    def running?
      !stop_event.set?
    end

    # @!macro [attach] executor_method_shuttingdown_question
    #
    #   Is the executor shuttingdown?
    #
    #   @return [Boolean] `true` when not running and not shutdown, else `false`
    def shuttingdown?
      !(running? || shutdown?)
    end

    # @!macro [attach] executor_method_shutdown_question
    #
    #   Is the executor shutdown?
    #
    #   @return [Boolean] `true` when shutdown, `false` when shutting down or running
    def shutdown?
      stopped_event.set?
    end

    # @!macro [attach] executor_method_shutdown
    #
    #   Begin an orderly shutdown. Tasks already in the queue will be executed,
    #   but no new tasks will be accepted. Has no additional effect if the
    #   thread pool is not running.
    def shutdown
      synchronize do
        break unless running?
        self.ns_auto_terminate = false
        stop_event.set
        shutdown_execution
      end
      true
    end

    # @!macro [attach] executor_method_kill
    #
    #   Begin an immediate shutdown. In-progress tasks will be allowed to
    #   complete but enqueued tasks will be dismissed and no new tasks
    #   will be accepted. Has no additional effect if the thread pool is
    #   not running.
    def kill
      synchronize do
        break if shutdown?
        self.ns_auto_terminate = false
        stop_event.set
        kill_execution
        stopped_event.set
      end
      true
    end

    # @!macro [attach] executor_method_wait_for_termination
    #
    #   Block until executor shutdown is complete or until `timeout` seconds have
    #   passed.
    #
    #   @note Does not initiate shutdown or termination. Either `shutdown` or `kill`
    #     must be called before this method (or on another thread).
    #
    #   @param [Integer] timeout the maximum number of seconds to wait for shutdown to complete
    #
    #   @return [Boolean] `true` if shutdown complete or false on `timeout`
    def wait_for_termination(timeout = nil)
      stopped_event.wait(timeout)
    end

    protected

    attr_reader :stop_event, :stopped_event

    # @!macro [attach] executor_method_execute
    def execute(*args, &task)
      raise NotImplementedError
    end

    # @!macro [attach] executor_method_shutdown_execution
    #
    #   Callback method called when an orderly shutdown has completed.
    #   The default behavior is to signal all waiting threads.
    def shutdown_execution
      stopped_event.set
    end

    # @!macro [attach] executor_method_kill_execution
    #
    #   Callback method called when the executor has been killed.
    #   The default behavior is to do nothing.
    def kill_execution
      # do nothing
    end
  end

  if Concurrent.on_jruby?

    # @!macro executor_service
    class JavaExecutorService < Synchronization::Object
      include Executor
      java_import 'java.lang.Runnable'

      # The set of possible fallback policies that may be set at thread pool creation.
      FALLBACK_POLICIES = {
        abort:       java.util.concurrent.ThreadPoolExecutor::AbortPolicy,
        discard:     java.util.concurrent.ThreadPoolExecutor::DiscardPolicy,
        caller_runs: java.util.concurrent.ThreadPoolExecutor::CallerRunsPolicy
      }.freeze

      def initialize
        super()
      end

      # @!macro executor_method_post
      def post(*args, &task)
        raise ArgumentError.new('no block given') unless block_given?
        return handle_fallback(*args, &task) unless running?
        executor_submit = @executor.java_method(:submit, [Runnable.java_class])
        executor_submit.call { yield(*args) }
        true
      rescue Java::JavaUtilConcurrent::RejectedExecutionException
        raise RejectedExecutionError
      end

      # @!macro executor_method_left_shift
      def <<(task)
        post(&task)
        self
      end

      # @!macro executor_method_running_question
      def running?
        !(shuttingdown? || shutdown?)
      end

      # @!macro executor_method_shuttingdown_question
      def shuttingdown?
        if @executor.respond_to? :isTerminating
          @executor.isTerminating
        else
          false
        end
      end

      # @!macro executor_method_shutdown_question
      def shutdown?
        @executor.isShutdown || @executor.isTerminated
      end

      # @!macro executor_method_wait_for_termination
      def wait_for_termination(timeout = nil)
        if timeout.nil?
          ok = @executor.awaitTermination(60, java.util.concurrent.TimeUnit::SECONDS) until ok
          true
        else
          @executor.awaitTermination(1000 * timeout, java.util.concurrent.TimeUnit::MILLISECONDS)
        end
      end

      # @!macro executor_method_shutdown
      def shutdown
        self.ns_auto_terminate = false
        @executor.shutdown
        nil
      end

      # @!macro executor_method_kill
      def kill
        self.ns_auto_terminate = false
        @executor.shutdownNow
        nil
      end
    end
  end

  if Concurrent.on_jruby?
    # @!macro executor_service
    class ExecutorService < JavaExecutorService
    end
  else
    # @!macro executor_service
    class ExecutorService < RubyExecutorService
    end
  end
end
