module Puma

  # Defines a nascent rack-metal protocol for the output.
  #
  # .headers - A Hash-like container of the headers to send back
  # .status - The status code number for the response
  # .content_length - If known, the length of the body. If not set, we'll
  #                   attempt to infer it. This implementation falls back
  #                   to using chunked encoding when the content-length
  #                   is unknown.
  # .write_header - When called, writes the full header back to the client
  # .write - Write one chunk of data back to the client
  # .write_many - Write many chunks of data back to the client
  # .flush - Flush any buffered data
  #

  class RackIO
    include Puma::Const

    def initialize(env, client, lines)
      @env = env
      @client = client
      @lines = lines

      @status = 0
      @content_length = nil
      @headers = nil

      @state = nil
      @chunked = false
      @no_body = false
    end

    attr_accessor :status, :headers, :content_length
    attr_reader :state

    def socket
      @client
    end

    def write_header
      lines = @lines
      status = @status
      env = @env
      client = @client

      cork_socket client

      no_body = env[REQUEST_METHOD] == HEAD

      line_ending = LINE_END
      colon = COLON

      status_name = HTTP_STATUS_CODES.fetch(status, 'CUSTOM')

      if env[HTTP_VERSION] == HTTP_11
        allow_chunked = true
        keep_alive = env[HTTP_CONNECTION] != CLOSE
        include_keepalive_header = false

        # An optimization. The most common response is 200, so we can
        # reply with the proper 200 status without having to compute
        # the response header.
        #
        if @status == 200
          lines << HTTP_11_200
        else
          lines.append "HTTP/1.1 ", status.to_s, " ", status_name, line_ending

          no_body ||= status < 200 || STATUS_WITH_NO_ENTITY_BODY[status]
        end
      else
        allow_chunked = false
        keep_alive = env[HTTP_CONNECTION] == KEEP_ALIVE
        include_keepalive_header = keep_alive

        # Same optimization as above for HTTP/1.1
        #
        if status == 200
          lines << HTTP_10_200
        else
          lines.append "HTTP/1.0 ", status.to_s, " ", status_name, line_ending

          no_body ||= status < 200 || STATUS_WITH_NO_ENTITY_BODY[status]
        end
      end

      content_length = @content_length

      headers.each do |k, vs|
        case k
        when CONTENT_LENGTH2
          content_length = vs
          next
        when TRANSFER_ENCODING
          allow_chunked = false
          content_length = nil
        end

        if vs.respond_to?(:to_s)
          vs.to_s.split(NEWLINE).each do |v|
            lines.append k, colon, v, line_ending
          end
        else
          lines.append k, colon, line_ending
        end
      end

      if no_body
        if content_length and status != 204
          lines.append CONTENT_LENGTH_S, content_length.to_s, line_ending
        end

        lines << line_ending
        fast_write client, lines.to_s

        @no_body = true
        @state = keep_alive unless @state

        return
      end

      if include_keepalive_header
        lines << CONNECTION_KEEP_ALIVE
      elsif !keep_alive
        lines << CONNECTION_CLOSE
      end

      if content_length
        lines.append CONTENT_LENGTH_S, content_length.to_s, line_ending
        chunked = false
      elsif allow_chunked
        lines << TRANSFER_ENCODING_CHUNKED
        chunked = true
      end

      lines << line_ending

      fast_write client, lines.to_s

      @state = keep_alive unless @state
      @chunked = chunked
    end

    def hijack
      @state = :async
      @socket
    end

    def write_many(chunks)
      return if @no_body

      chunked = @chunked
      client = @client

      line_ending = LINE_END

      chunks.each do |part|
        if chunked
          fast_write client, part.bytesize.to_s(16)
          fast_write client, line_ending
          fast_write client, part
          fast_write client, line_ending
        else
          fast_write client, part
        end

        client.flush
      end
    rescue SystemCallError, IOError
      raise ConnectionError, "Connection error detected during write"
    end

    def write(chunk)
      if @chunked
        client = @client

        fast_write client, part.bytesize.to_s(16)
        fast_write client, line_ending
        fast_write client, part
        fast_write client, line_ending
      elsif @no_body
        return
      else
        fast_write client, part
      end
    rescue SystemCallError, IOError
      raise ConnectionError, "Connection error detected during write"
    end

    alias_method :<<, :write
    alias_method :syswrite, :write

    def flush
    end

    def finalize
      begin
        if @chunked
          fast_write @client, CLOSE_CHUNKED
          @client.flush
        elsif @no_body
          return
        end
      rescue SystemCallError, IOError
        raise ConnectionError, "Connection error detected during write"
      ensure
        uncork_socket @client
      end
    end

    # On Linux, use TCP_CORK to better control how the TCP stack
    # packetizes our stream. This improves both latency and throughput.
    #
    if RUBY_PLATFORM =~ /linux/
      # 6 == Socket::IPPROTO_TCP
      # 3 == TCP_CORK
      # 1/0 == turn on/off
      def cork_socket(socket)
        begin
          socket.setsockopt(6, 3, 1) if socket.kind_of? TCPSocket
        rescue IOError, SystemCallError
        end
      end

      def uncork_socket(socket)
        begin
          socket.setsockopt(6, 3, 0) if socket.kind_of? TCPSocket
        rescue IOError, SystemCallError
        end
      end
    else
      def cork_socket(socket)
      end

      def uncork_socket(socket)
      end
    end

    def fast_write(io, str)
      n = 0
      while true
        begin
          n = io.syswrite str
        rescue Errno::EAGAIN, Errno::EWOULDBLOCK
          if !IO.select(nil, [io], nil, WRITE_TIMEOUT)
            raise ConnectionError, "Socket timeout writing data"
          end

          retry
        rescue  Errno::EPIPE, SystemCallError, IOError
          raise ConnectionError, "Socket timeout writing data"
        end

        return if n == str.bytesize
        str = str.byteslice(n..-1)
      end
    end
    private :fast_write
  end
end
