require 'logger'

# Loggerをシングルトンで使うためのクラスです。
class CustomLogger
    @@logger = nil

    # Loggerインスタンスを取得します。
    def self.get_logger
        if @@logger == nil
            @@logger = Logger.new( STDOUT )
            @@logger.formatter = proc do | severity, datetime, progname, msg |
                "#{datetime}:#{sprintf( "%7s", "[#{severity}]" )} -- #{msg}\n"
            end
        end
        @@logger
    end

end