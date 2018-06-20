module Coverband
  module Adapters
    class BaseStore
      def initialize(store, opts = {})
        @store = store
      end

      def clear!
        raise "Implement in subclass"
      end

    end
  end
end
