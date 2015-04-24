require 'concurrent/errors'
require 'concurrent/logging'
require 'concurrent/at_exit'
require 'concurrent/synchronization'
require 'concurrent/atomic/event'

module Concurrent

  module Executor
    include Logging

    # Get the requested `Executor` based on the values set in the options hash.
    #
    # @param [Hash] opts the options defining the requested executor
    # @option opts [Executor] :executor when set use the given `Executor` instance.
    #   Three special values are also supported: `:fast` returns the global fast executor,
    #   `:io` returns the global io executor, and `:immediate` returns a new
    #   `ImmediateExecutor` object.
    #
    # @return [Executor, nil] the requested thread pool, or nil when no option specified
    #
    # @!visibility private
    def self.executor_from_options(opts = {}) # :nodoc:
      case
      when opts.key?(:executor)
        if opts[:executor].nil?
          nil
        else
          executor(opts[:executor])
        end
      when opts.key?(:operation) || opts.key?(:task)
        if opts[:operation] == true || opts[:task] == false
          Kernel.warn '[DEPRECATED] use `executor: :fast` instead'
          return Concurrent.global_fast_executor
        end

        if opts[:operation] == false || opts[:task] == true
          Kernel.warn '[DEPRECATED] use `executor: :io` instead'
          return Concurrent.global_io_executor
        end

        raise ArgumentError.new("executor '#{opts[:executor]}' not recognized")
      else
        nil
      end
    end

    def self.executor(executor_identifier)
      case executor_identifier
      when :fast
        Concurrent.global_fast_executor
      when :io
        Concurrent.global_io_executor
      when :immediate
        Concurrent.global_immediate_executor
      when :operation
        Kernel.warn '[DEPRECATED] use `executor: :fast` instead'
        Concurrent.global_fast_executor
      when :task
        Kernel.warn '[DEPRECATED] use `executor: :io` instead'
        Concurrent.global_io_executor
      when Executor
        executor_identifier
      else
        raise ArgumentError, "executor not recognized by '#{executor_identifier}'"
      end
    end

    # The policy defining how rejected tasks (tasks received once the
    # queue size reaches the configured `max_queue`, or after the
    # executor has shut down) are handled. Must be one of the values
    # specified in `FALLBACK_POLICIES`.
    attr_reader :fallback_policy

    def auto_terminate?
      synchronize { ns_auto_terminate? }
    end

    def auto_terminate=(value)
      synchronize { self.ns_auto_terminate = value }
    end

    # @!macro [attach] executor_module_method_can_overflow_question
    #
    #   Does the task queue have a maximum size?
    #
    #   @return [Boolean] True if the task queue has a maximum size else false.
    #
    # @note Always returns `false`
    def can_overflow?
      false
    end

    # Handler which executes the `fallback_policy` once the queue size
    # reaches `max_queue`.
    #
    # @param [Array] args the arguments to the task which is being handled.
    #
    # @!visibility private
    def handle_fallback(*args)
      case @fallback_policy
      when :abort
        raise RejectedExecutionError
      when :discard
        false
      when :caller_runs
        begin
          yield(*args)
        rescue => ex
          # let it fail
          log DEBUG, ex
        end
        true
      else
        fail "Unknown fallback policy #{@fallback_policy}"
      end
    end

    # @!macro [attach] executor_module_method_serialized_question
    #
    #   Does this executor guarantee serialization of its operations?
    #
    #   @return [Boolean] True if the executor guarantees that all operations
    #     will be post in the order they are received and no two operations may
    #     occur simultaneously. Else false.
    #
    # @note Always returns `false`
    def serialized?
      false
    end

    private

    def ns_auto_terminate?
      !!@auto_terminate
    end

    def ns_auto_terminate=(value)
      case value
      when true
        AtExit.add(self) { terminate_at_exit }
        @auto_terminate = true
      when false
        AtExit.delete(self)
        @auto_terminate = false
      else
        raise ArgumentError
      end
    end

    def terminate_at_exit
      kill # TODO be gentle first
      wait_for_termination(10)
    end
  end

  # Indicates that the including `Executor` or `ExecutorService` guarantees
  # that all operations will occur in the order they are post and that no
  # two operations may occur simultaneously. This module provides no
  # functionality and provides no guarantees. That is the responsibility
  # of the including class. This module exists solely to allow the including
  # object to be interrogated for its serialization status.
  #
  # @example
  #   class Foo
  #     include Concurrent::SerialExecutor
  #   end
  #
  #   foo = Foo.new
  #
  #   foo.is_a? Concurrent::Executor       #=> true
  #   foo.is_a? Concurrent::SerialExecutor #=> true
  #   foo.serialized?                      #=> true
  module SerialExecutor
    include Executor

    # @!macro executor_module_method_serialized_question
    #
    # @note Always returns `true`
    def serialized?
      true
    end
  end
end
