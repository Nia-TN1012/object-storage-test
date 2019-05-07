require 'httpclient'
require 'json'
require 'date'
require_relative '../logger/custom-logger'

# ConoHa APIのトークンの取得と管理を行います。
class ConoHaAPIClient

    # エンドポイント
    CONOHA_ENDPOINT = "https://identity.tyo1.conoha.io/v2.0"
    # configファイルのディレクトリ
    CONFIG_DIR = "#{File.dirname( __FILE__ )}/../../config"
    # dataファイルのディレクトリ
    DATA_DIR = "#{File.dirname( __FILE__ )}/../../data"
    # Logger
    @@logger = CustomLogger::get_logger

    # 設定ファイルを読み込み、クラスを初期化します。
    #
    # パラメーター:
    #   conf_file: 設定ファイル名（JSON形式）（configフォルダ内）
    #   is_force_request_token: トークンを強制的に再取得
    #
    # 設定ファイルの書式:
    # {
    #    "api_user": "APIユーザー名",
    #    "api_pass": "APIパスワード",
    #    "tenant_id": "テナントID"
    # }
    def initialize( conf_file = "default", is_force_request_token = false )
        @@logger.info( "[#{self.class.name}] Init." )

        json_data = open( "#{CONFIG_DIR}/#{conf_file}.json" ) do | io |
            JSON.load( io )
        end

        @api_user = json_data['api_user']
        @api_pass = json_data['api_pass']
        @tenant_id = json_data['tenant_id']

        load_token( is_force_request_token )

        if @api_token == nil
            get_token
        end
    end

    private
    # APIユーザーに関連する、保存済みのトークン情報をロードします。
    #
    # パラメーター:
    #   is_force_request_token: トークンを強制的に再取得
    #
    # トークン情報ファイルの書式:
    # {
    #    "api_token": "APIトークン",
    #    "expire_date": "APIトークンの有効期限"
    # }
    def load_token( is_force_request_token )
        @@logger.info( "[#{self.class.name}] Loading token data. ( id: #{@api_user} )" )
        if File.exist?( "#{DATA_DIR}/token_#{@api_user}.json" )
            json_data = open( "#{DATA_DIR}/token_#{@api_user}.json" ) do | io |
                JSON.load( io )
            end
            api_token_expire_date = DateTime.parse( json_data['expire_date'] )
            # 有効期限内であれば、保存済みのトークンを使用します。（強制再取得フラグの時を除く）
            if !is_force_request_token && DateTime.now < api_token_expire_date
                @api_token = json_data['api_token']
            else
                @api_token = nil
            end
        else
            @api_token = nil
        end
    end

    # ConoHaのAPIで、トークンを発行します。
    # https://www.conoha.jp/docs/identity-post_tokens.html
    def get_token
        @@logger.info( "[#{self.class.name}] Getting token. ( id: #{@api_user} )" )
        params = {
            'auth' => {
                'passwordCredentials' => {
                    'username' => @api_user,
                    'password' => @api_pass
                },
                'tenantId' => @tenant_id
            }
        }.to_json
        http_client = HTTPClient.new;
        res = http_client.post( "#{CONOHA_ENDPOINT}/tokens", body: params, header: {'Accept' => "application/json"} )
        if res.status != HTTP::Status::OK
            @@logger.fatal( "[#{self.class.name}] Failed to get Access Token. (HTTP #{res.status})" )
            raise
        end

        json_data = JSON.parse( res.body )

        @api_token = json_data['access']['token']['id']
        api_token_expire_date = json_data['access']['token']['expires']
        token_data = {
            'api_token' => @api_token,
            'expire_date' => api_token_expire_date
        }.to_json

        File.open( "#{DATA_DIR}/token_#{@api_user}.json", "w" ) do | file | 
            file.puts token_data
        end

        @@logger.info( "[#{self.class.name}] Got Access Token ( id: #{@api_user} )" )
    end
end