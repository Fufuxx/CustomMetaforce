module Metaforce
  #FB - New Class crud - Do not extends -> Own JOb --> SaveResult[] 4 CLASSES DeleteResult[] / ReadResult[]
  # REmove all together - No use for Doppel
  class Job::CRUD < Job
    def initialize(client, method, args)
      super(client)
      @method, @args = method, args
    end

    def perform
      @id = @client.send(@method, *@args).id
      super
    end
  end
end
