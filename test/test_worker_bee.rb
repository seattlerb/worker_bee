require "minitest/autorun"
require "worker_bee"

class TestWorkerBee < MiniTest::Unit::TestCase
  def test_sanity_manual
    bee = WorkerBee.new

    bee.input(*(1..25).to_a)

    bee.work(20) { |n| sleep 0.25; n ** 2 }
    bee.finish
    bee.work(5)  { |n| Math.sqrt n }
    bee.finish

    expected = (1..25).to_a
    assert_equal expected, bee.results.map(&:to_i).sort
  end

  def test_sanity_automatic
    bee = WorkerBee.new

    bee.input(*(1..25).to_a)

    bee.work(20) { |n| n ** 2 }
    # bee.finish # commented out on purpose
    bee.work(5)  { |n| Math.sqrt n }

    expected = (1..25).to_a
    assert_equal expected, bee.results.map(&:to_i).sort
  end
end
