module Metaforce
  module Metadata
    class Client
      module File

        # Public: Specify an array of component types to list.
        #
        # Examples
        #
        #   # Get a list of apex classes on the server and output the names of each
        #   client.list_metadata('ApexClass').collect { |t| t.full_name }
        #   #=> ["al__SObjectPaginatorListenerForTesting", "al__IndexOutOfBoundsException", ... ]
        #
        #   # Get a list of apex components and apex classes
        #   client.list_metadata('CustomObject', 'ApexComponent')
        #   #=> ["ContractContactRole", "Solution", "Invoice_Statements__c", ... ]
        def list_metadata(*args)
          queries = args.map(&:to_s).map(&:camelize).map { |t| {:type => t} }
          request :list_metadata do |soap|
            soap.body = { :queries => queries }
          end
        end

        # Public: Describe the organization's metadata.
        #
        # version - API version (default: latest).
        #
        # Examples
        #
        #   # List the names of all metadata types
        #   client.describe.metadata_objects.collect { |t| t.xml_name }
        #   #=> ["CustomLabels", "StaticResource", "Scontrol", "ApexComponent", ... ]
        def describe(version=nil)
          request :describe_metadata do |soap|
            soap.body = { :api_version => version } unless version.nil?
          end
        end

        # New Status - one for Deploy / one for Retrieve v40.0
        def checkDeployStatus(id, details='true')
          request :check_deploy_status do |soap|
            soap.body = { :asyncProcessId => id, :includeDetails => details }
          end
        end

        def checkRetrieveStatus(id, zip='false')
          request :check_retrieve_status do |soap|
            soap.body = { :asyncProcessId => id, :include_zip => zip }
          end
        end

        # Public: Deploy code to Salesforce.
        #
        # zip_file - The base64 encoded contents of the zip file.
        # options  - Hash of DeployOptions.
        #
        # Returns the AsyncResult
        #FB - Not using Job !!
        def _deploy(zip_file, options={})
          request :deploy do |soap|
            soap.body = { :zip_file => zip_file, :deploy_options => options }
          end
        end

        # Public: Retrieve code from Salesforce.
        #
        # Returns the AsyncResult
        def _retrieve(options={})
          request :retrieve do |soap|
            soap.body = { :retrieve_request => options }
          end
        end

        # Public: Deploy code to Salesforce.
        #
        # path    - A path to a zip file, or a directory to deploy.
        # options - Deploy options.
        #
        # Examples
        #
        #   client.deploy(File.expand_path('./src'))
        #FB - Rewrite with new classes
        def deploy(path, options={})
          Job::Deploy.new(self, path, options)
        end

        def retrieve(options={})
          #Job::Retrieve.new(self, options)
          p "Retrieve class call"
          RetrieveJob.new(self, options)
        end

        # Public: Retrieves files specified in the manifest file (A package.xml
        # file).
        def retrieve_unpackaged(manifest, options={})
          p "Retrieve Unpackage metaforce"
          package = if manifest.is_a?(Metaforce::Manifest)
            manifest
          elsif manifest.is_a?(String)
            Metaforce::Manifest.new(::File.open(manifest).read)
          end
          options = {
            :api_version    => Metaforce.configuration.api_version,
            :single_package => true,
            :unpackaged     => { :types => package.to_package }
          }.merge(options)
          retrieve(options)
        end

      end
    end
  end
end
