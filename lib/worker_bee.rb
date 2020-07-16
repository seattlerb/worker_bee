require 'thread'

##
# Defines a WorkerBee instance which provides a simple means of
# defining pipelines of parallel tasks working from one queue of work
# to a queue of results (which can then be a queue of work for another
# pipeline of workers).
#
#     WorkerBee.new(self) # the self is important for context in `then` calls
#       .input(*urls)
#       .then(:url_to_api)
#       .then(:api_to_ary, n:20)
#       .flatten
#       .then(:issues_to_url)
#       .compact
#       .results
#       .sort

class WorkerBee
  VERSION = "1.0.0"     # :nodoc:
  SENTINAL = Object.new # :nodoc:

  ##
  # A slightly better Queue.

  class BQ < Queue
    ##
    # Initialize a BQ queue with +objs+.

    def initialize objs = []
      super()
      concat objs
    end

    ##
    # Push multiple objects into the queue

    def concat objs
      objs.each do |obj|
        self << obj
      end
    end

    ##
    # Close and return all data as an array.

    def drain
      close

      result = []
      result.push self.shift until self.empty?
      result
    end
  end

  ##
  # The queue of queues of tasks to perform.

  attr_accessor :tasks

  ##
  # Pipelines of workers. Each worker is wired up to task queues directly.

  attr_accessor :workers

  ##
  # The context for the current run. This is only important for +then+.

  attr_accessor :context

  attr_accessor :count # :nodoc: internal counter only

  ##
  # Creates a new WorkerBee with one queue and no pipelines of workers.

  def initialize context = nil
    self.tasks   = [BQ.new]
    self.workers = []
    self.context = context || self
    self.count   = 0
  end

  def next_count # :nodoc:
    self.count += 1
    self.count
  end

  ##
  # Add work to the front of the pipeline.

  def input *work
    tasks.first.concat work
    self
  end

  ##
  # A generic worker bee. Does work in the input queue and puts it
  # into the output queue until it gets the +SENTINAL+ value.

  class Worker < Thread
    ##
    # The input queue of work

    attr_accessor :input

    ##
    # The output queue of work

    attr_accessor :output

    ##
    # The actual work to do (a proc).

    attr_accessor :work

    ##
    # Initialize and start a worker.

    def initialize input, output, &work
      self.input  = input
      self.output = output
      self.work   = work

      self.abort_on_exception = true

      super() do
        loop do
          task = input.shift

          break if task == SENTINAL

          call task
        end
      end
    end

    ##
    # Do +work+ on +task+ and put the result into the +output+ queue.

    def call task
      output << work[task]
    end
  end

  ##
  # Add a pipeline of work with +n+ parallel workers of a certain
  # +type+ (defaulting to Worker) performing +block+ as the task for
  # this pipeline.

  def work n = 1, type:Worker, &block
    input  = tasks[self.count]
    output = tasks[self.next_count] = BQ.new

    workers << n.times.map { type.new input, output, &block }

    self
  end

  alias :toil :work
  alias :slog :work

  ##
  # A worker that filters non-truthy results of work from the next pipeline.

  class CompactWorker < Worker
    ##
    # Do +work+ on +task+ and put the truthy results into the +output+
    # queue.

    def call task
      result = work[task]
      output << result if result
    end
  end

  ##
  # Remove all non-truthy tasks.

  def compact
    work klass:CompactWorker, &:itself
  end

  ##
  # A worker that flattens the results of work into the next pipeline.

  class FlattenWorker < Worker
    ##
    # Do +work+ on +task+ and put the (multiple) results into the
    # +output+ queue.

    def call task
      work[task].each do |out|
        output << out
      end
    end
  end

  ##
  # Flatten out all tasks. Lets you have one task create arrays of
  # subtasks.

  def flatten n:1, &work
    work n, klass:CompactWorker, &:itself
  end

  ##
  # A worker that passes task through only if the work is truthy.

  class Filter < Worker
    def call task
      output << task if work[task]
    end
  end

  ##
  # Filter task out if +work+ is doesn't evaluate to truthy. Eg:
  #
  #   bee.input(*Dir["**/*"])
  #   bee.filter(1) { |path| File.file? path }
  #   bee.filter(4) { |path| `file -b #{path}` =~ /Ruby script/ }
  #   ...

  def filter n, &work
    work n, type:Filter, &work
  end

  ##
  # Convenience function:
  #
  #   bee.then :msg_name
  #
  # is a shortcut equivalent to:
  #
  #   bee.work(n) { |task| msg_name task }

  def then msg_name, n:1
    work(n) { |obj| context.send msg_name, obj }
  end

  ##
  # Finish all work, from front to back.

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
  # Finish and return the contents of all work.

  def results
    finish

    tasks[count].drain - [SENTINAL]
  end
end
