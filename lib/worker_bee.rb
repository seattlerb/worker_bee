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
#
# Helper methods:
#
# * input *data
# * trap_interrupt!
# * periodic time = 5, &update
# * work n:1, &block
# * compact
# * flatten
# * filter n:1, &work
# * then n:1, msg_name
# * results

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

  ##
  # Threads that run parallel to everything else. Can be used to
  # show progress or do periodic cleanup. Killed by #finish.

  attr_accessor :updaters

  ##
  # Creates a new WorkerBee with one queue and no pipelines of workers.

  def initialize context = nil
    self.tasks    = [BQ.new]
    self.workers  = []
    self.context  = context || self
    self.updaters = []
  end

  ##
  # The front of the pipeline

  def front
    tasks.first
  end

  ##
  # The current back of the pipeline

  def back
    tasks.last
  end

  ##
  # Add another queue to back of the pipeline.

  def add_to_pipeline
    tasks << BQ.new
    back
  end

  ##
  # Add +data+ to the front of the pipeline of tasks.

  def input *data
    front.concat data
    self
  end

  ##
  # Meant to be used in an INT trap. Clears the remaining input and
  # then finishes all work.

  def interrupted!
    non_empty = tasks.find { |pool| ! pool.empty? }
    non_empty.clear if non_empty

    trap "INT", "EXIT"

    finish
  end

  ##
  # Set up an interrupt handler that cleanly finishes.

  def trap_interrupt!
    trap("INT") { $stderr.puts "finishing..."; interrupted! }
    self
  end

  ##
  # Schedules a side task to run every +time+ seconds parallel to
  # everything else. Shut down by #finish.

  def periodic time = 5, &update
    self.updaters << Thread.new do
      loop do
        update.call self
        sleep time
      end
    end
    self
  end

  ##
  # Returns the current counts of all tasks in the pipeline.

  def counts
    tasks.zip(workers).map { |t, w| t.size + (w || []).count(&:working?) }
  end

  ##
  # A generic worker bee. Does work in the input queue and puts it
  # into the output queue until it gets the +SENTINAL+ value.

  class Worker < Thread
    ##
    # True if working on a task
    attr_accessor :working
    alias working? working

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
      self.working = false

      self.abort_on_exception = true

      super() do
        loop do
          task = input.shift

          break if task == SENTINAL

          self.working = true
          call task
          self.working = false
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

  def work n:1, type:Worker, &block
    input  = back
    output = add_to_pipeline

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
      out = work[task]
      output << out if out
    end
  end

  ##
  # Remove all non-truthy tasks.

  def compact
    work type:CompactWorker, &:itself
  end

  ##
  # A worker that flattens the results of work into the next pipeline.

  class FlattenWorker < Worker
    ##
    # Do +work+ on +task+ and put the (multiple) results into the
    # +output+ queue.

    def call task
      output.concat work[task]
    end
  end

  ##
  # Flatten out all tasks. Lets you have one task create arrays of
  # subtasks.

  def flatten
    work type:FlattenWorker, &:itself
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
  #   bee.filter      { |path| File.file? path }
  #   bee.filter(n:4) { |path| `file -b #{path}` =~ /Ruby script/ }
  #   ...

  def filter n:1, &work
    work n:n, type:Filter, &work
  end

  def grep arg, n:1
    work(n:n) { |data| data.grep arg }
  end

  def map arg, n:1
    work(n:n) { |data| data.send arg }
  end

  def grep_v_clear arg, n:1
    work(n:n) { |data| data.grep_v(arg).each(&:clear); data }
  end

  def non_empty n:1
    work(n:n) { |data| data.reject(&:empty?) }
  end

  def match arg, n:1
    work(n:n) { |data| arg === data }
  end

  ##
  # Convenience function:
  #
  #   bee.then :msg_name, n:3
  #
  # is a shortcut equivalent to:
  #
  #   bee.work(3) { |task| msg_name task }

  def then msg_name, n:1
    m = context.method(msg_name) rescue nil
    if m then
      work(n:n) { |obj| context.send msg_name, obj }
    else
      warn "warning: prototyping #{msg_name}"
      work(n:n) { |obj| obj }
    end
  end

  ##
  # Finish all work on all tasks, from front to back.

  def finish
    # TODO: zip workers and tasks?

    workers.each do |pool|
      input = pool.first.input

      pool.size.times do
        input << SENTINAL
      end

      pool.each do |thread|
        thread.join
      end
    end

    updaters.each(&:kill)
  end

  ##
  # Finish and return the contents of all work.

  def results
    finish

    back.drain - [SENTINAL]
  end
end
