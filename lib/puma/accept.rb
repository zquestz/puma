require 'socket'

module Puma
  if TCPServer.method_defined? :accept_nonblock2
    def self.try_accept(sock)
      sock.accept_nonblock2
    end
  else
    def self.try_accept(sock)
      begin
        sock.accept_nonblock
      rescue Errno::EAGAIN
        return nil
      end
    end
  end
end
