require 'httpclient'
require 'json'
require 'date'
require_relative './conoha-api'

# ConoHaのオブジェクトストレージを管理します。
class ObjectStrageClient < ConoHaAPIClient

    # エンドポイント
    CONOHA_ENDPOINT = "https://object-storage.tyo1.conoha.io/v1"

    # クラスを初期化します。
    def initialize( conf_file = "default", is_force_request_token = false )
        super( conf_file, is_force_request_token )

        @@logger.info( "[#{self.class.name}] Ready." )
    end
    
    # オブジェクトストレージの情報を取得します。
    # https://www.conoha.jp/docs/swift-show_account_details_and_list_containers.html
    #
    # 戻り値:
    #   ・コンテナリストの配列（取得に成功）
    #   ・空配列（取得に成功したが、コンテナが存在しない）
    #   ・nil（取得に失敗）
    def get_info
        headers = {
            'Accept' => "application/json",
            'X-Auth-Token' => @api_token
        }
        http_client = HTTPClient.new;
        res = http_client.get( "#{CONOHA_ENDPOINT}/nc_#{@tenant_id}", header: headers )
        if res.status >= HTTP::Status::BAD_REQUEST
            @@logger.error( "[#{self.class.name}] Failed to get Object Storage info. (HTTP #{res.status})" )
            return nil
        end

        json_data = JSON.parse( res.body )
    end

    # オブジェクトストレージのコンテナ情報を取得します。
    # https://www.conoha.jp/docs/swift-show_container_details_and_list_objects.html
    #
    # パラメーター:
    #   container_name: コンテナ名（コンテナのリストはget_infoメソッドで取得できます。）
    #
    # 戻り値:
    #   ・オブジェクトリストの配列（取得に成功）
    #   ・空配列（取得に成功したが、オブジェクトが存在しない）
    #   ・nil（取得に失敗）
    def get_container_info( container_name )
        headers = {
            'Accept' => "application/json",
            'X-Auth-Token' => @api_token
        }
        http_client = HTTPClient.new;
        res = http_client.get( "#{CONOHA_ENDPOINT}/nc_#{@tenant_id}/#{container_name}", header: headers )
        if res.status >= HTTP::Status::BAD_REQUEST
            @@logger.error( "[#{self.class.name}] Failed to get container ('#{container_name}') info. (HTTP #{res.status})" )
            return nil
        end

        json_data = JSON.parse( res.body )
    end

    # オブジェクトストレージのオブジェクト情報を取得します。
    # https://www.conoha.jp/docs/swift-get_object_content_and_metadata.html
    #
    # パラメーター:
    #   container_name: コンテナ名（コンテナのリストはget_infoメソッドで取得できます。）
    #   object_name:    オブジェクト名（オブジェクトのリストはget_container_infoメソッドで取得できます。）
    #
    # 戻り値:
    #   ・オブジェクトの情報（取得に成功）
    #   ・nil（取得に失敗）
    def get_object_info( container_name, object_name )
        headers = {
            'Accept' => "application/json",
            'X-Auth-Token' => @api_token
        }
        http_client = HTTPClient.new;
        res = http_client.get( "#{CONOHA_ENDPOINT}/nc_#{@tenant_id}/#{container_name}/#{object_name}", header: headers )
        if res.status != HTTP::Status::OK
            @@logger.error( "[#{self.class.name}] Failed to get object ('#{container_name}/#{object_name}') info. (HTTP #{res.status})" )
            return nil
        end
        
        res.headers
    end

    # コンテナを作成します。
    # https://www.conoha.jp/docs/swift-create_container.html
    #
    # パラメーター:
    #   container_name: 作成するコンテナの名前
    #
    # 戻り値:
    #   ・レスポンスヘッダ（作成に成功（すでに存在する場合、警告メッセーが出力されます。））
    #   ・nil（作成に失敗）
    def create_container( container_name )
        headers = {
            'Accept' => "application/json",
            'X-Auth-Token' => @api_token
        }
        http_client = HTTPClient.new;
        @@logger.info( "[#{self.class.name}] Creating container: '#{container_name}'" )
        res = http_client.put( "#{CONOHA_ENDPOINT}/nc_#{@tenant_id}/#{container_name}", header: headers )
        if res.status == HTTP::Status::CREATED
            @@logger.info( "[#{self.class.name}] Create container completed: '#{container_name}'" )
            res.headers
        elsif res.status == HTTP::Status::ACCEPTED  # コンテナがすでに存在する場合、HTTP 202が返ります。
            @@logger.warn( "[#{self.class.name}] Container ('#{container_name}') is already exist." )
            res.headers
        else
            @@logger.error( "[#{self.class.name}] Failed to create container ('#{container_name}'). (HTTP #{res.status})" )
            nil
        end
    end

    # コンテナを削除します。（注: あらかじめ、削除するコンテナの中身を空にする必要があります）
    # https://www.conoha.jp/docs/swift-delete_container.html
    #
    # パラメーター:
    #   container_name: 削除するコンテナの名前（コンテナのリストはget_infoメソッドで取得できます。）
    #
    # 戻り値:
    #   ・レスポンスヘッダ（削除に成功）
    #   ・nil（削除に失敗）
    def delete_container( container_name )
        headers = {
            'Accept' => "application/json",
            'X-Auth-Token' => @api_token
        }
        http_client = HTTPClient.new;
        @@logger.info( "[#{self.class.name}] Deleting container: '#{container_name}'" )
        res = http_client.delete( "#{CONOHA_ENDPOINT}/nc_#{@tenant_id}/#{container_name}", header: headers )
        if res.status == HTTP::Status::NO_CONTENT
            @@logger.info( "[#{self.class.name}] Delete container completed: '#{container_name}'" )
            res.headers
        elsif res.status == 409     # 削除しようとするコンテナの中にオブジェクトがあると、HTTP 409が返ります。
            @@logger.error( "[#{self.class.name}] Cannot delete container ('#{container_name}'). Because some objects exist in the container." )
            nil
        else
            @@logger.error( "[#{self.class.name}] Failed to delete container ('#{container_name}'). (HTTP #{res.status})" )
            nil
        end
    end

    # 指定したファイルをオブジェクトストレージにアップロードします。
    # https://www.conoha.jp/docs/swift-object_upload.html
    #
    # パラメーター:
    #   input_file:     アップロードするファイルパス（現在のディレクトリからの相対パスでもOK）
    #   content_type:   アップロードするファイルのMIME名
    #   container_name: コンテナの名前（コンテナのリストはget_infoメソッドで取得できます。）
    #   object_name:    オブジェクト名（省略時、アップロードするファイルの名前（拡張子付き）になります。）
    #
    # 戻り値:
    #   ・アップロードしたオブジェクトの情報（アップロードに成功）
    #   ・nil（アップロードに失敗）
    def upload_object( input_file, content_type, container_name, object_name = nil )
        if !File.exist?( input_file )
            @@logger.error( "[#{self.class.name}] File '#{input_file}' not found." )
            return nil
        end

        object_name ||= File.basename( input_file )
        headers = {
            'Accept' => "application/json",
            'Content-Type' => content_type,
            'X-Auth-Token' => @api_token
        }
        http_client = HTTPClient.new;
        @@logger.info( "[#{self.class.name}] Uploading object: '#{input_file}' > '#{container_name}/#{object_name}'" )
        res = http_client.put( "#{CONOHA_ENDPOINT}/nc_#{@tenant_id}/#{container_name}/#{object_name}", body: File.open( input_file ), header: headers )

        if res.status >= HTTP::Status::BAD_REQUEST
            @@logger.error( "[#{self.class.name}] Failed to upload '#{input_file}' to '#{container_name}/#{object_name}'. (HTTP #{res.status})" )
            return nil
        end
        @@logger.info( "[#{self.class.name}] Upload object completed: '#{container_name}/#{object_name}'" )

        get_object_info( container_name, object_name )
    end

    # 指定したオブジェクトをダウンロードします。
    # https://www.conoha.jp/docs/swift-object_download.html
    #
    # パラメーター:
    #   container_name: コンテナの名前（コンテナのリストはget_infoメソッドで取得できます。）
    #   object_name:    オブジェクト名（オブジェクトのリストはget_container_infoメソッドで取得できます。）
    #   output_file:    出力ファイルパス（省略時、ファイル名はcontainer_nameとして、現在のディレクトリ内に保存します。）
    #
    # 戻り値:
    #   ・ダウンロードしたオブジェクトの情報（ダウンロードに成功）
    #   ・nil（ダウンロードに失敗）
    def download_object( container_name, object_name, output_file = nil )
        @@logger.info( "[#{self.class.name}] Checking: '#{container_name}/#{object_name}'" )
        object_info = get_object_info( container_name, object_name )
        if object_info == nil
            @@logger.error( "[#{self.class.name}] Object '#{container_name}/#{object_name}' not found." )
            return nil
        end
        @@logger.info( "[#{self.class.name}] Object '#{container_name}/#{object_name}' found." )

        output_file ||= "./#{object_name}"
        headers = {
            'Accept' => "application/json",
            'X-Auth-Token' => @api_token
        }
        http_client = HTTPClient.new;
        http_client.receive_timeout = 60 * 120
        @@logger.info( "[#{self.class.name}] Downloading object: '#{container_name}/#{object_name}'" )
        open( output_file, 'wb') do | io |
            http_client.get_content( "#{CONOHA_ENDPOINT}/nc_#{@tenant_id}/#{container_name}/#{object_name}", header: headers ) do | chunk |
                io.write chunk
            end
        end

        @@logger.info( "[#{self.class.name}] Download object completed: '#{container_name}/#{object_name}' > '#{output_file}'" )
        object_info
    end

    # オブジェクトを削除します。
    # https://www.conoha.jp/docs/swift-delete_object.html
    #
    # パラメーター:
    #   container_name: コンテナの名前（コンテナのリストはget_infoメソッドで取得できます。）
    #   object_name:    オブジェクト名（オブジェクトのリストはget_container_infoメソッドで取得できます。）
    #
    # 戻り値:
    #   ・レスポンスヘッダ（削除に成功）
    #   ・nil（削除に失敗）
    def delete_object( container_name, object_name )
        headers = {
            'Accept' => "application/json",
            'X-Auth-Token' => @api_token
        }
        http_client = HTTPClient.new;
        @@logger.info( "[#{self.class.name}] Deleting object: '#{container_name}/#{object_name}'" )
        res = http_client.delete( "#{CONOHA_ENDPOINT}/nc_#{@tenant_id}/#{container_name}/#{object_name}", header: headers )

        if res.status == HTTP::Status::NO_CONTENT
            @@logger.info( "[#{self.class.name}] Delete object completed: '#{container_name}/#{object_name}'" )
            res.headers
        else
            @@logger.error( "[#{self.class.name}] Failed to delete object ('#{container_name}/#{object_name}'). (HTTP #{res.status})" )
            nil
        end
    end

end