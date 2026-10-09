# frozen_string_literal: true

require "spec_helper"
require "pg"
require_relative "../driver/riverqueue-sequel/spec/spec_helper"

RSpec.describe River::Driver::InsertListener, database: :postgres do
  before { skip "Postgres unavailable" unless DB }

  let(:interrupt_pipe) { IO.pipe }
  let(:interrupt) { interrupt_pipe.first }

  def notify(payload, schema: SEQUEL_TEST_SCHEMA)
    DB.synchronize { |connection| connection.exec_params("SELECT pg_notify($1, $2)", ["#{schema}.river_insert", payload]) }
  end

  def listen
    River::Driver::Sequel.new(DB).listen_for_inserts { |listener| yield listener }
  end

  after { interrupt_pipe.each(&:close) }

  it "listens on the insert channel of its connection's schema" do
    result = listen do |listener|
      notify("other schema", schema: "public")
      notify(JSON.generate(queue: "one"))
      notify(JSON.generate(queue: "two"))

      payloads = []
      payloads.concat(listener.wait(5, interrupt)) until payloads.length == 2
      expect(payloads).to eq([JSON.generate(queue: "one"), JSON.generate(queue: "two")])
    end

    expect(result).to be true
  end

  it "returns notifications the connection received outside a wait" do
    listen do |listener|
      notify(JSON.generate(queue: "one"))
      # The idle ping reads notifications committed before it along with its result.
      listener.instance_variable_get(:@connection).exec("SELECT 1")
      interrupt_pipe.last.write(".")

      expect(listener.wait(5, interrupt)).to eq([JSON.generate(queue: "one")])
    end
  end

  it "returns early when interrupted and pings an idle connection" do
    listen do |listener|
      interrupt_pipe.last.write(".")
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      expect(listener.wait(5, interrupt)).to be_empty
      expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be < 1
      interrupt.read_nonblock(1)

      expect(listener.wait(0.01, interrupt)).to be_empty
    end
  end

  it "raises when its connection is terminated" do
    listen do |listener|
      pid = listener.instance_variable_get(:@connection).backend_pid
      DB.fetch("SELECT pg_terminate_backend(?)", pid).all

      expect { 2.times { listener.wait(5, interrupt) } }.to raise_error(PG::Error)
    end
  end
end
