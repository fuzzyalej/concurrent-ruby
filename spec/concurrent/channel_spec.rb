require 'spec_helper'
require_relative 'runnable_shared'

module Concurrent

  describe Channel do

    subject { Channel.new }
    let(:runnable) { Channel }

    it_should_behave_like :runnable

    after(:each) do
      subject.stop
      @thread.kill unless @thread.nil?
      sleep(0.1)
    end

    context '#post' do

      it 'returns false when not running' do
        subject.post.should be_false
      end

      it 'pushes a message onto the queue' do
        @expected = false
        channel = Channel.new{|msg| @expected = msg }
        @thread = Thread.new{ channel.run }
        @thread.join(0.1)
        channel.post(true)
        @thread.join(0.1)
        @expected.should be_true
        channel.stop
      end

      it 'returns the current size of the queue' do
        channel = Channel.new{|msg| sleep }
        @thread = Thread.new{ channel.run }
        @thread.join(0.1)
        channel.post(true).should == 1
        @thread.join(0.1)
        channel.post(true).should == 1
        @thread.join(0.1)
        channel.post(true).should == 2
        channel.stop
      end

      it 'is aliased a <<' do
        @expected = false
        channel = Channel.new{|msg| @expected = msg }
        @thread = Thread.new{ channel.run }
        @thread.join(0.1)
        channel << true
        @thread.join(0.1)
        @expected.should be_true
        channel.stop
      end
    end

    context '#run' do

      it 'empties the queue' do
        @thread = Thread.new{ subject.run }
        @thread.join(0.1)
        q = subject.instance_variable_get(:@queue)
        q.size.should == 0
      end
    end

    context '#stop' do

      it 'empties the queue' do
        channel = Channel.new{|msg| sleep }
        @thread = Thread.new{ channel.run }
        10.times { channel.post(true) }
        @thread.join(0.1)
        channel.stop
        @thread.join(0.1)
        q = channel.instance_variable_get(:@queue)
        if q.size >= 1
          q.pop.should == :stop
        else
          q.size.should == 0
        end
      end

      it 'pushes a :stop message onto the queue' do
        @thread = Thread.new{ subject.run }
        @thread.join(0.1)
        q = subject.instance_variable_get(:@queue)
        q.should_receive(:push).once.with(:stop)
        subject.stop
        @thread.join(0.1)
      end
    end

    context 'message handling' do

      it 'runs the constructor block once for every message' do
        @expected = 0
        channel = Channel.new{|msg| @expected += 1 }
        @thread = Thread.new{ channel.run }
        @thread.join(0.1)
        10.times { channel.post(true) }
        @thread.join(0.1)
        @expected.should eq 10
        channel.stop
      end

      it 'passes the message to the block' do
        @expected = []
        channel = Channel.new{|msg| @expected << msg }
        @thread = Thread.new{ channel.run }
        @thread.join(0.1)
        10.times {|i| channel.post(i) }
        @thread.join(0.1)
        channel.stop
        @expected.should eq (0..9).to_a
      end
    end

    context 'exception handling' do

      it 'supresses exceptions thrown when handling messages' do
        channel = Channel.new{|msg| raise StandardError }
        @thread = Thread.new{ channel.run }
        expect {
          @thread.join(0.1)
          10.times { channel.post(true) }
        }.not_to raise_error
        channel.stop
      end

      it 'calls the errorback with the time, message, and exception' do
        @expected = []
        errorback = proc{|*args| @expected = args }
        channel = Channel.new(errorback){|msg| raise StandardError }
        @thread = Thread.new{ channel.run }
        @thread.join(0.1)
        channel.post(42)
        @thread.join(0.1)
        @expected[0].should be_a(Time)
        @expected[1].should == [42]
        @expected[2].should be_a(StandardError)
        channel.stop
      end
    end

    context 'observer notification' do

      let(:observer) do
        Class.new {
          attr_reader :notice
          def update(*args) @notice = args; end
        }.new
      end

      it 'notifies observers when a message is successfully handled' do
        observer.should_receive(:update).exactly(10).times.with(any_args())
        subject.add_observer(observer)
        @thread = Thread.new{ subject.run }
        @thread.join(0.1)
        10.times { subject.post(true) }
        @thread.join(0.1)
      end

      it 'does not notify observers when a message raises an exception' do
        observer.should_not_receive(:update).with(any_args())
        channel = Channel.new{|msg| raise StandardError }
        channel.add_observer(observer)
        @thread = Thread.new{ channel.run }
        @thread.join(0.1)
        10.times { channel.post(true) }
        @thread.join(0.1)
        channel.stop
      end

      it 'passes the time, message, and result to the observer' do
        channel = Channel.new{|*msg| msg }
        channel.add_observer(observer)
        @thread = Thread.new{ channel.run }
        @thread.join(0.1)
        channel.post(42)
        @thread.join(0.1)
        observer.notice[0].should be_a(Time)
        observer.notice[1].should == [42]
        observer.notice[2].should == [42]
        channel.stop
      end
    end

    context '#pool' do

      let(:clazz){ Class.new(Channel) }

      it 'raises an exception if the count is zero or less' do
        expect {
          clazz.pool(0)
        }.to raise_error(ArgumentError)
      end
  
      it 'creates the requested number of channels' do
        mailbox, channels = clazz.pool(5)
        channels.size.should == 5
      end

      it 'passes the errorback to each channel' do
        errorback = proc{ nil }
        clazz.should_receive(:new).with(errorback)
        clazz.pool(1, errorback)
      end

      it 'passes the block to each channel' do
        block = proc{ nil }
        clazz.should_receive(:new).with(anything(), &block)
        clazz.pool(1, nil, &block)
      end

      it 'gives all channels the same mailbox' do
        mailbox, channels = clazz.pool(2)
        mbox1 = channels.first.instance_variable_get(:@queue)
        mbox2 = channels.last.instance_variable_get(:@queue)
        mbox1.should eq mbox2
      end

      it 'returns a Poolbox as the first retval' do
        mailbox, channels = clazz.pool(2)
        mailbox.should be_a(Channel::Poolbox)
      end

      it 'gives the Poolbox the same mailbox as the channels' do
        mailbox, channels = clazz.pool(1)
        mbox1 = mailbox.instance_variable_get(:@queue)
        mbox2 = channels.first.instance_variable_get(:@queue)
        mbox1.should eq mbox2
      end

      it 'returns an array of channels as the second retval' do
        mailbox, channels = clazz.pool(2)
        channels.each do |channel|
          channel.should be_a(clazz)
        end
      end

      it 'posts to the mailbox with Poolbox#post' do
        @expected = false
        mailbox, channels = clazz.pool(1){|msg| @expected = true }
        @thread = Thread.new{ channels.first.run }
        sleep(0.1)
        mailbox.post(42)
        sleep(0.1)
        channels.each{|channel| channel.stop }
        @thread.kill
        @expected.should be_true
      end

      it 'posts to the mailbox with Poolbox#<<' do
        @expected = false
        mailbox, channels = clazz.pool(1){|msg| @expected = true }
        @thread = Thread.new{ channels.first.run }
        sleep(0.1)
        mailbox << 42
        sleep(0.1)
        channels.each{|channel| channel.stop }
        @thread.kill
        @expected.should be_true
      end
    end

    context 'subclassing' do

      after(:each) do
        @thread.kill unless @thread.nil?
      end

      context '#pool' do

        it 'creates channels of the appropriate subclass' do
          actor = Class.new(Channel)
          mailbox, channels = actor.pool(1)
          channels.first.should be_a(actor)
        end
      end

      context '#receive overloading' do

        let(:actor) do
          Class.new(Channel) {
            attr_reader :last_message
            def receive(*message)
              @last_message = message
            end
          }
        end

        it 'ignores the constructor block' do
          @expected = false
          channel = actor.new{|*args| @expected = true }
          @thread = Thread.new{ channel.run }
          @thread.join(0.1)
          channel.post(:foo)
          @thread.join(0.1)
          @expected.should be_false
          channel.stop
        end

        it 'uses the subclass receive implementation' do
          channel = actor.new{|*args| @expected = true }
          @thread = Thread.new{ channel.run }
          @thread.join(0.1)
          channel.post(:foo)
          @thread.join(0.1)
          channel.last_message.should eq [:foo]
          channel.stop
        end
      end

      context '#on_error overloading' do

        let(:actor) do
          Class.new(Channel) {
            attr_reader :last_error
            def receive(*message)
              raise StandardError
            end
            def on_error(*args)
              @last_error = args
            end
          }
        end

        it 'ignores the constructor errorback' do
          @expected = false
          errorback = proc{|*args| @expected = true }
          channel = actor.new(errorback)
          @thread = Thread.new{ channel.run }
          @thread.join(0.1)
          channel.post(true)
          @thread.join(0.1)
          @expected.should be_false
          channel.stop
        end

        it 'uses the subclass #on_error implementation' do
          channel = actor.new
          @thread = Thread.new{ channel.run }
          @thread.join(0.1)
          channel.post(42)
          @thread.join(0.1)
          channel.last_error[0].should be_a(Time)
          channel.last_error[1].should eq [42]
          channel.last_error[2].should be_a(StandardError)
          channel.stop
        end
      end
    end

    context 'supervision' do

      it 'can be started by a Supervisor' do
        channel = Channel.new
        supervisor = Supervisor.new
        supervisor.add_worker(channel)
        
        channel.should_receive(:run).with(no_args())
        supervisor.run!
        sleep(0.1)

        supervisor.stop
        sleep(0.1)
        channel.stop
      end

      it 'can receive messages while under supervision' do
        @expected = false
        channel = Channel.new{|*args| @expected = true}
        supervisor = Supervisor.new
        supervisor.add_worker(channel)
        supervisor.run!
        sleep(0.1)

        channel.post(42)
        sleep(0.1)
        @expected.should be_true

        supervisor.stop
        sleep(0.1)
        channel.stop
      end

      it 'can be stopped by a supervisor' do
        channel = Channel.new
        supervisor = Supervisor.new
        supervisor.add_worker(channel)
        
        supervisor.run!
        sleep(0.1)

        channel.should_receive(:stop).with(no_args())
        supervisor.stop
        sleep(0.1)
      end
    end
  end
end