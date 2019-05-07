require './lib/conoha/conoha-objs'

#objs = ObjectStrageClient.new
#p objs.create_container( "mysqlbackup" )
#p objs.get_container_info( "mysqlbackup" )
#p objs.upload_object( "./mysql.sql.gz", "application/octet-stream", "mysqlbackup" )
#p objs.download_object( "mysqlbackup", "mysql.sql.gz", "./mysql2.sql.gz" )
#p objs.get_object_info( "mysqlbackup", "mysql.sql.gz" )
#p objs.delete_object( "mysqlbackup", "mysql.sql.gz" )