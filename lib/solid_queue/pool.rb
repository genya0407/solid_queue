# frozen_string_literal: true

module SolidQueue
  class Pool
    include AppExecutor

    attr_reader :thread_size, :extra_claim_size

    delegate :shutdown, :shutdown?, :wait_for_termination, to: :executor

    def initialize(thread_size:, extra_claim_size:, on_idle: nil)
      @thread_size = thread_size
      @extra_claim_size = extra_claim_size
      @on_idle = on_idle
      @mutex = Mutex.new
    end

    def post(execution)
      Concurrent::Promises.future_on(executor, execution) do |thread_execution|
        wrap_in_app_executor do
          thread_execution.perform
        ensure
          mutex.synchronize { on_idle.try(:call) if idle? }
        end
      end.on_rejection! do |e|
        handle_thread_error(e)
      end
    end

    def claim_size
      executor.max_queue - executor.queue_length
    end

    def idle?
      executor.queue_length <= executor.length
    end

    private
      attr_reader :on_idle, :mutex

      DEFAULT_OPTIONS = {
        min_threads: 0,
        idletime: 60,
        fallback_policy: :abort
      }

      def executor
        @executor ||= Concurrent::ThreadPoolExecutor.new DEFAULT_OPTIONS.merge(max_threads: thread_size, max_queue: thread_size + extra_claim_size)
      end
  end
end
