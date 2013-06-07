require 'harq/client'

module Puma
  class HarqConnection
    def initialize(host, port, path)
      @client = Harq::Client.new host, port
      @client.make_transient path
      @client.subscribe! path
    end

    def to_io
      @client.to_io
    end

    class Header
      include Beefcake::Message

      module Key
        HOST = 0
        ACCEPT = 1
        USER_AGENT = 2
      end

      RackKeys = {
        0 => "HTTP_HOST",
        1 => "HTTP_ACCEPT",
        2 => "HTTP_USER_AGENT"
      }

      optional :key, Key, 1
      optional :custom_key, :string, 2
      required :value, :string, 3

      def rack_key
        if key
          RackKeys[key]
        else
          "HTTP_" + custom_key.gsub("-","_").upcase
        end
      end
    end

    class Request
      include Beefcake::Message

      module Method
        DELETE = 0
        GET = 1
        HEAD = 2
        POST = 3
        PUT = 4
      end

      required :version_major, :uint32, 1
      required :version_minor, :uint32, 2
      required :stream_id, :uint32, 8

      optional :method, Method, 3
      optional :custom_method, :string, 4
      required :url, :string, 5
      repeated :headers, Header, 6
      optional :body, :bytes, 7
    end

    class Response
      include Beefcake::Message

      required :stream_id, :uint32, 1
      required :status, :uint32, 2
      repeated :headers, Header, 3
      optional :body, :bytes, 4
    end

    class Client
      def initialize(conn, msg, env)
        @connection = conn
        @message = msg
        @env = env
      end

      def io
        self
      end

      def peeraddr
        ["127.0.0.1"]
      end

      attr_reader :env

      def eagerly_finish
        req = Request.decode @message.payload

        @request = req

        import_headers

        true
      end

      def hijacked
        false
      end

      def import_headers
        @request.headers.each do |h|
          env[h.rack_key] = h.value
        end

        env[Const::REQUEST_PATH] = @request.url
      end

      def body
        StringIO.new(@request.body || "")
      end

      def close
        nil
      end

      def stream_id
        @request.stream_id
      end

      def send_response(status, headers, res_body)

        headers = headers.map { |k,v| Header.new :custom_key => k, :value => v }
        body = res_body.join("")

        rep = Response.new :stream_id => stream_id,
                           :status => status.to_i,
                           :headers => headers,
                           :body => body

        p rep
        @connection.queue rep

        false
      end
    end

    def read_client(env)
      p :read_client
      Client.new self, @client.read_message, env
    end

    def queue(rep)
      str = ""
      rep.encode(str)
      @client.queue "/harq-http/reply", str
    end

  end
end
