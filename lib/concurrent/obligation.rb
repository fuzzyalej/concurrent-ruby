require 'thread'
require 'timeout'

require 'concurrent/dereferenceable'
require 'concurrent/atomic/event'

module Concurrent

  module Obligation
    include Dereferenceable

    # Has the obligation been fulfilled?
    #
    # @return [Boolean]
    def fulfilled?
      state == :fulfilled
    end
    alias_method :realized?, :fulfilled?

    # Has the obligation been rejected?
    #
    # @return [Boolean]
    def rejected?
      state == :rejected
    end

    # Is obligation completion still pending?
    #
    # @return [Boolean]
    def pending?
      state == :pending
    end

    # Is the obligation still unscheduled?
    #
    # @return [Boolean]
    def unscheduled?
      state == :unscheduled
    end

    # Has the obligation completed processing?
    #
    # @return [Boolean]
    def complete?
      [:fulfilled, :rejected].include? state
    end

    # Has the obligation completed processing?
    #
    # @return [Boolean]
    #
    # @deprecated
    def completed?
      warn '[DEPRECATED] Use #complete? instead'
      complete?
    end

    # Is the obligation still awaiting completion of processing?
    #
    # @return [Boolean]
    def incomplete?
      ! complete?
    end

    # The current value of the obligation. Will be `nil` while the state is
    # pending or the operation has been rejected.
    #
    # @param [Numeric] timeout the maximum time in seconds to wait.
    # @return [Object] see Dereferenceable#deref
    def value(timeout = nil)
      wait timeout
      deref
    end

    # Wait until obligation is complete or the timeout has been reached.
    #
    # @param [Numeric] timeout the maximum time in seconds to wait.
    # @return [Obligation] self
    def wait(timeout = nil)
      event.wait(timeout) if timeout != 0 && incomplete?
      self
    end

    # Wait until obligation is complete or the timeout is reached. Will re-raise
    # any exceptions raised during processing (but will not raise an exception
    # on timeout).
    #
    # @param [Numeric] timeout the maximum time in seconds to wait.
    # @return [Obligation] self
    # @raise [Exception] raises the reason when rejected
    def wait!(timeout = nil)
      wait(timeout).tap { raise self if rejected? }
    end
    alias_method :no_error!, :wait!

    # The current value of the obligation. Will be `nil` while the state is
    # pending or the operation has been rejected. Will re-raise any exceptions
    # raised during processing (but will not raise an exception on timeout).
    #
    # @param [Numeric] timeout the maximum time in seconds to wait.
    # @return [Object] see Dereferenceable#deref
    # @raise [Exception] raises the reason when rejected
    def value!(timeout = nil)
      wait(timeout)
      if rejected?
        raise self
      else
        deref
      end
    end

    # The current state of the obligation.
    #
    # @return [Symbol] the current state
    def state
      mutex.synchronize { @state }
    end

    # If an exception was raised during processing this will return the
    # exception object. Will return `nil` when the state is pending or if
    # the obligation has been successfully fulfilled.
    #
    # @return [Exception] the exception raised during processing or `nil`
    def reason
      mutex.synchronize { @reason }
    end

    # @example allows Obligation to be risen
    #   rejected_ivar = Ivar.new.fail
    #   raise rejected_ivar
    def exception(*args)
      raise 'obligation is not rejected' unless rejected?
      reason.exception(*args)
    end

    protected

    # @!visibility private
    def get_arguments_from(opts = {}) # :nodoc:
      [*opts.fetch(:args, [])]
    end

    # @!visibility private
    def init_obligation(*args) # :nodoc:
      init_mutex(*args)
      @event = Event.new
    end

    # @!visibility private
    def event # :nodoc:
      @event
    end

    # @!visibility private
    def set_state(success, value, reason) # :nodoc:
      if success
        @value = value
        @state = :fulfilled
      else
        @reason = reason
        @state  = :rejected
      end
    end

    # @!visibility private
    def state=(value) # :nodoc:
      mutex.synchronize { @state = value }
    end

    # Atomic compare and set operation
    # State is set to `next_state` only if `current state == expected_current`.
    #
    # @param [Symbol] next_state
    # @param [Symbol] expected_current
    # 
    # @return [Boolean] true is state is changed, false otherwise
    #
    # @!visibility private
    def compare_and_set_state(next_state, expected_current) # :nodoc:
      mutex.synchronize do
        if @state == expected_current
          @state = next_state
          true
        else
          false
        end
      end
    end

    # executes the block within mutex if current state is included in expected_states
    #
    # @return block value if executed, false otherwise
    #
    # @!visibility private
    def if_state(*expected_states) # :nodoc:
      mutex.synchronize do
        raise ArgumentError.new('no block given') unless block_given?

        if expected_states.include? @state
          yield
        else
          false
        end
      end
    end
  end
end
