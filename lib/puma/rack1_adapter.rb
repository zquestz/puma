module Puma
  class Rack1Adapter
    include Puma::Const

    def initialize(app, options, events)
      @app = app
      @options = options
      @events = events
    end

    def handle(env, input, output)
      env[PUMA_SOCKET] = output.socket

      hijacked = false

      env[HIJACK_P] = true
      env[HIJACK] = lambda do
        hijacked = true
        env[HIJACK_IO] ||= output.hijack
      end

      head = env[REQUEST_METHOD] == HEAD

      env[RACK_INPUT] = input
      env[RACK_URL_SCHEME] =  env[HTTPS_KEY] ? HTTPS : HTTP

      # A rack extension. If the app writes #call'ables to this
      # array, we will invoke them when the request is done.
      #
      after_reply = env[RACK_AFTER_REPLY] = []

      begin
        begin
          status, headers, res_body = @app.call(env)

          return if hijacked

          status = status.to_i

          if status == -1
            unless headers.empty? and res_body == []
              raise "async response must have empty headers and body"
            end

            return :async
          end
        rescue StandardError => e
          @events.unknown_error self, e, "Rack app"

          status, headers, res_body = lowlevel_error(e)
        end

        output.status = status
        output.headers = headers

        if res_body.kind_of? Array and res_body.size == 1
          output.content_length = res_body[0].bytesize
        end

        output.write_header

        output.write_many res_body
      ensure
        input.close
        res_body.close if res_body.respond_to? :close

        after_reply.each { |o| o.call }
      end
    end

    def lowlevel_error(e)
      if handler = @options[:lowlevel_error_handler]
        return handler.call(e)
      end

      if @leak_stack_on_error
        [500, {}, ["Puma caught this error: #{e.message} (#{e.class})\n#{e.backtrace.join("\n")}"]]
      else
        [500, {}, ["A really lowlevel plumbing error occured. Please contact your local Maytag(tm) repair man.\n"]]
      end
    end

  end
end
