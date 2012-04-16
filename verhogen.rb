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
      process_response(res)
    end

    def get(path)
      res = Net::HTTP.new(@host, @port).get("#{path}.json")
      process_response(res)
    end

    # Issue a fake DELETE method request using the conventional _method param.
    def delete(path, params = {})
      post(path, params.merge("_method" => "delete"))
    end

    def mutex(opts = {})
      Verhogen::Mutex.new(opts.merge(:client => self))
    end


    ############################################################
    # Private Methods
    ############################################################
    private

    def process_response(response)
      res = JSON.parse(response.body)
      if res['error']
        raise res['message'] || res['error']
      end
      return res
    rescue JSON::ParserError => e
      raise "UnknownError"
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

    def holding_lock?
      @holding_lock
    end

    def destroy
      resp = client.delete("/mutexes/#{@uuid}")
      @client = nil # So that we can't try to use this instance anymore
      true
    end


    ############################################################
    # Private Instance Methods
    ############################################################
    private

    def create_if_necessary
      resp = if @uuid
               # Info info from server about this Mutex instance
               client.get("/mutexes/#{@uuid}")
             else
               # Create a new Mutex instance on server
               client.post("/mutexes", {:name => @name})
             end
      @uuid         = resp['uuid']
      @name         = resp['name']
      @holding_lock = resp['holdingLock']
    end
  end

end
