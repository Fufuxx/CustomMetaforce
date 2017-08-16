require 'zip'
require 'base64'

module Metaforce
  class RetrieveJob
    DELAY_START = 1
    DELAY_MULTIPLIER = 2

    # Public: The id of the AsyncResult returned from Salesforce for
    # this job.
    attr_reader :id

    # Public: Instantiate a new job. Doesn't actually do anything until
    # .perform is called.
    #
    # Examples
    #
    #   job = Metaforce::Job.new(client)
    #   # => #<Metaforce::Job @id=nil>
    #
    # Returns self.
    def initialize(client)
      @_callbacks = Hash.new { |h,k| h[k] = [] }
      @client = client
    end

    # Public: Perform the job.
    #
    # Examples
    #
    #   job = Metaforce::Job.new
    #   job.perform
    #   # => #<Metaforce::Job @id=nil>
    #
    # Returns self.
    def perform
      start_heart_beat
      self
    end

    # Public: Utility method to determine if .perform has been called yet.
    #
    # Returns true if @id is set, false otherwise.
    def started?
      !!@id
    end

    # Public: Register a block to be called when an event occurs.
    #
    # Yields the job.
    #
    # &block - Proc or Lambda to be run when the event is triggered.
    #
    # Examples
    #
    #   job.on_complete do |job|
    #     puts "Job ##{job.id} completed!"
    #   end
    #
    #   job.on_error do |job|
    #     puts "Job failed!"
    #   end
    #
    #   job.on_poll do |job|
    #     puts "Polled status for #{job.id}"
    #   end
    #
    # Returns self.
    #
    # Signature
    #
    #   on_complete(&block)
    #   on_error(&block)
    #   on_poll(&block)
    %w[complete error poll].each do |type|
      define_method :"on_#{type}" do |&block|
        @_callbacks[:"on_#{type}"] << block
        self
      end
    end

    # Public: Queries the job status from the API.
    #
    # Examples
    #
    #   job.status
    #   # => { :id => '1234', :done => false, ... }
    #
    # Returns the AsyncResult (http://www.salesforce.com/us/developer/docs/api_meta/Content/meta_asyncresult.htm).
    def status
      @status ||= client.checkRetrieveStatus(id)
    end

    # Public: Returns true if the job has completed.
    #
    # Examples
    #
    #   job.done
    #   # => true
    #
    # Returns true if the job has completed, false otherwise.
    def done?
      status.done
    end

    # Public: Returns the state if the job has finished processing.
    #
    # Examples
    #
    #   job.state
    #   # => 'Completed'
    #
    # Returns the state of the job.
    def state
      status.status
    end

    # Public: Get the detailed status of the retrieve.
    #
    # Examples
    #
    #   job.result
    #   # => { :id => '1234', :zip_file => '<base64 encoded content>', ... }
    #
    # Returns the RetrieveResult (http://www.salesforce.com/us/developer/docs/api_meta/Content/meta_retrieveresult.htm).
    def result
      @result ||= client.checkRetrieveStatus(id)
    end

    # Public: Decodes the content of the returned zip file.
    #
    # Examples
    #
    #   job.zip_file
    #   # => '<binary content>'
    #
    # Returns the decoded content.
    def zip_file
      Base64.decode64(result.zip_file)
    end

    # Public: Unzips the returned zip file to the location.
    #
    # destination - Path to extract the contents to.
    #
    # Examples
    #
    #   job.extract_to('./path')
    #   # => #<Metaforce::Job::Retrieve @id='1234'>
    #
    # Returns self.
    def extract_to(destination)
      return on_complete { |job| job.extract_to(destination) } unless started?
      with_tmp_zip_file do |file|
        unzip(file, destination)
      end
      self
    end

    # Public: Check if the job is in a given state.
    #
    # Examples
    #
    #   job.queued?
    #   # => false
    #
    # Returns true or false.
    #
    # Signature
    #
    #   queued?
    #   in_progress?
    #   completed?
    #   error?
    %w[Pending InProgress Succeeded Failed].each do |state|
      define_method :"#{state.underscore}?" do; self.state == state end
    end

    def inspect
      "#<#{self.class} @id=#{@id.inspect}>"
    end

    def self.disable_threading!
      ActiveSupport::Deprecation.warn <<-WARNING.strip_heredoc
        Metaforce::Job.disable_threading! is deprecated. Use Metaforce.configuration.threading = false instead.
      WARNING
      Metaforce.configuration.threading = false
    end

  private
    attr_reader :client

    # Internal: Starts a heart beat in a thread, which polls the job status
    # until it has completed or timed out.
    def start_heart_beat
      if threading?
        Thread.abort_on_exception = true
        @heart_beat ||= Thread.new &run_loop
      else
        run_loop.call
      end
    end

    # Internal: Starts the run loop, and blocks until the job has completed or
    # failed.
    def run_loop
      proc {
        delay = DELAY_START
        loop do
          @status = nil
          sleep (delay = delay * DELAY_MULTIPLIER)
          trigger :on_poll
          if succeeded? || failed?
            trigger callback_type
            Thread.stop if threading?
            break
          end
        end
      }
    end

    def trigger(type)
      @_callbacks[type].each do |block|
        block.call(self)
      end
    end

    def callback_type
      if succeeded?
        :on_complete
      elsif failed?
        :on_error
      end
    end

    def threading?
      Metaforce.configuration.threading
    end

    # Internal: Unzips source to destination.
    def unzip(source, destination)
      Zip::File.open(source) do |zip|
        zip.each do |f|
          path = File.join(destination, f.name)
          FileUtils.mkdir_p(File.dirname(path))
          zip.extract(f, path) { true }
        end
      end
    end

    # Internal: Writes the zip file content to a temporary location so it can
    # be extracted.
    def with_tmp_zip_file
      file = Tempfile.new('retrieve')
      begin
        file.binmode
        file.write(zip_file)
        file.rewind
        yield file
      ensure
        file.close
        file.unlink
      end
    end

  end
end
