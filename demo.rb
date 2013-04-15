require 'celluloid'

module Celluloid
  class Coordinator
    include Celluloid

    def initialize(destination)
      @destination = destination
      @backlog = []
      @futures = []
    end

    def enqueue(epic)
      if @running
        @backlog << epic
      else
        @current = epic.to_a
        3.times do
          async.dequeue
        end
      end
    end

    def dequeue
      if work = @current.shift
        Celluloid::Logger.info "sending work: #{work.inspect}"
        result = @destination.sync(:perform, work)
        Celluloid::Logger.info "got result: #{result.inspect}"
        async.dequeue
      else
        Celluloid::Logger.info "no more work"
      end
    end
  end

  class Reiterator
    def initialize(ary)
      @iterator = ary.each
    end

    def next
      @iterator.next
    rescue StopIteration
      @iterator.rewind
      retry
    end
  end

  class Router
    include Celluloid

    def initialize(number, klass)
      @children = number.times.map do
        klass.new
      end
      @iterator = Reiterator.new(@children)
    end

    def method_missing(meth, *args, &block)
      @iterator.next.sync(meth, *args, &block)
    end
  end
end

class Worker
  include Celluloid

  def perform(work)
    Celluloid::Logger.info "starting #{work.inspect}"
    sleep 1 + rand(2)
    Celluloid::Logger.info "finished #{work.inspect}"
  end
end

router = Celluloid::Router.new(3, Worker)
coordinator = Celluloid::Coordinator.new(router)
coordinator.enqueue 200.times

sleep
