module Coverband
  module Adapters
    class KafkaStore
      def initialize(kafka, opts={})
        # Delivery boy instance?
        @kafka = kafka
      end

      def clear!
        "Clear/destroy topic?"
      end

      def save_report(report)
        if @store_as_array
          kafka.pipelined do
            store_array(BASE_KEY, report.keys)

            report.each do |file, lines|
              store_array("#{BASE_KEY}.#{file}", lines.keys)
            end
          end
        else
          store_array(BASE_KEY, report.keys)

          report.each do |file, lines|
            store_map("#{BASE_KEY}.#{file}", lines)
          end
        end
      end

      def coverage
        data = {}
        kafka.smembers(BASE_KEY).each do |key|
          data[key] = covered_lines_for_file(key)
        end
        data
      end

      def covered_files
        kafka.smembers(BASE_KEY)
      end

      def covered_lines_for_file(file)
        if @store_as_array
          @kafka.smembers("#{BASE_KEY}.#{file}").map(&:to_i)
        else
          @kafka.hgetall("#{BASE_KEY}.#{file}")
        end
      end

      private

      attr_reader :kafka

      def sadd_supports_array?
        @_sadd_supports_array
      end

      def store_map(key, values)
        unless values.empty?
          existing = kafka.hgetall(key)
          #in kafka all keys are strings
          values = Hash[values.map{|k,val| [k.to_s,val] } ]
          values.merge!( existing ){|k, old_v, new_v| old_v.to_i + new_v.to_i}
          kafka.mapped_hmset(key, values)
        end
      end

      def store_array(key, values)
        if sadd_supports_array?
          kafka.sadd(key, values) if (values.length > 0)
        else
          values.each do |value|
            kafka.sadd(key, value)
          end
        end
        values
      end

      def recent_server_version?
        info_data = kafka.info
        if info_data.is_a?(Hash)
          Gem::Version.new(info_data['kafka_version']) >= Gem::Version.new('2.4')
        else
          #guess supported
          true
        end
      end

      def recent_gem_version?
        Gem::Version.new(Redis::VERSION) >= Gem::Version.new('3.0')
      end

  end
end
