require 'thread'

##
# Defines a WorkerBee instance which provides a simple means of
# defining parallel tasks working from one queue of work to a queue of
# results (which can then be a queue of work for another pipeline of
# workers).
#
#     bee = WorkerBee.new
#     bee.add_work :input, *(1..1000).to_a
#
#     workers = bee.workers 20, :input, :square do |task|
#       task ** 2
#     end
#
#     workers.finish
#
#     workers = bee.workers 5, :square do |task|
#       Math.sqrt task
#     end
#
#     workers.finish
#
#     p bee.results

Thread.abort_on_exception = true

class WorkerBee
  VERSION = "1.0.0"
  SENTINAL = Object.new

  attr_reader :tasks, :workers
  attr_accessor :count

  ##
  # Creates a new WorkerBee.

  def initialize
    @tasks   = [Queue.new]
    @workers = []
    @count   = 0
  end

  def next_count
    self.count += 1
    self.count
  end

  def input *work
    q = tasks.first
    work.each do |task|
      q << task
    end
    self
  end

  class Worker < Thread
    attr_accessor :input, :output

    def initialize input, output, &b
      @input, @output = input, output
      super() do
        loop do
          task = input.shift

          break if task == SENTINAL

          output << b[task]
        end
      end
    end
  end

  def work n, &b
    input  = tasks[self.count]
    output = tasks[self.next_count] = Queue.new

    workers << (1..n).map { Worker.new input, output, &b }

    self
  end

  def finish
    workers.each do |pool|
      input = pool.first.input

      pool.size.times do
        input << SENTINAL
      end

      pool.each do |thread|
        thread.join
      end
    end
  end

  ##
  # Returns the contents of the queue +name+ (defaults to +:result+).

  def results
    finish

    q = tasks[count]

    result = []
    result.push q.shift until q.empty?
    result.delete SENTINAL
    result
  end
end
