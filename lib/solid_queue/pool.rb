# frozen_string_literal: true

module SolidQueue
  class Pool
    include AppExecutor

    attr_reader :thread_size, :extra_claim_size

    delegate :shutdown, :shutdown?, :wait_for_termination, to: :executor

    class SolidExecutor < Concurrent::ThreadPoolExecutor
      def initialize(options)
        @on_idle = options.delete(:on_idle)
        super(options)
      end

      def claim_size
        self.synchronize { max_queue - (queue_length + active_count) }
      end

      def immediately_executable_count
        self.synchronize { max_length - active_count }
      end

      def call_on_idle_if_claimable
        return unless @on_idle

        self.synchronize { @on_idle.call if queue_length <= 0 }
      end
    end  

    def initialize(thread_size:, extra_claim_size:, on_idle: nil)
      @thread_size = thread_size
      @extra_claim_size = extra_claim_size
      @on_idle = on_idle
    end

    def post(execution)
      Concurrent::Promises.future_on(executor, execution) do |thread_execution|
        wrap_in_app_executor do
          thread_execution.perform
        ensure
          executor.call_on_idle_if_claimable
        end
      end.on_rejection! do |e|
        handle_thread_error(e)
      end
    end

    def claim_size
      executor.max_queue - (executor.queue_length + executor.active_count)
    end

    def idle?
      executor.immediately_executable_count > 0
    end

    private
      attr_reader :on_idle, :mutex

      DEFAULT_OPTIONS = {
        min_threads: 0,
        idletime: 60,
        fallback_policy: :abort
      }

      def executor
        @executor ||= SolidExecutor.new DEFAULT_OPTIONS.merge(max_threads: thread_size, max_queue: thread_size + extra_claim_size, on_idle: on_idle)
      end
  end
end
