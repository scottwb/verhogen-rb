require 'uri'
require 'net/http'
require 'json'
require 'cgi'

module Verhogen
  class Client
    MAX_LOCK_TIMEOUT = 60 * 60 * 24  # 24 hours

    attr_reader :host, :port

    def initialize(opts = {})
      @host = opts[:host] || 'verhogen.com'
      @port = opts[:port] || 80
    end

    def post(path, params = {})
      payload = params.map{|k,v| "#{CGI::escape(k.to_s)}=#{CGI::escape(v)}"}.join('&')
      http = Net::HTTP.new(@host, @port)
      http.read_timeout = MAX_LOCK_TIMEOUT
      res = http.post("#{path}.json", payload)
      JSON.parse(res.body)
    end

    def mutex(opts = {})
      Verhogen::Mutex.new(opts.merge(:client => self))
    end
  end

  class Mutex
    attr_reader :client, :name, :uuid

    def initialize(opts = {})
      @client       = opts[:client]
      @name         = opts[:name]
      @uuid         = opts[:uuid]
      @holding_lock = false
      create_if_necessary
    end

    def acquire(timeout = 0)
      begin
        resp = client.post("/mutexes/#{@uuid}/acquire")
        status = resp['status']
        @holding_lock = true
        if block_given?
          begin
            yield
          ensure
            release
          end
        end
      rescue Timeout::Error => e
        retry
      end

      true
    end

    def release
      return false unless holding_lock?
      resp = client.post("/mutexes/#{@uuid}/release")
      status = resp['status']
      @holding_lock = false
      true
    end


    ############################################################
    # Private Instance Methods
    ############################################################
    private

    def create_if_necessary
      if @uuid.nil?
        resp = client.post("/mutexes", {:name => @name})
        @uuid = resp['id']
      end
    end

    def holding_lock?
      @holding_lock
    end
  end

end
