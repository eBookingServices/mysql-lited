module mysql.protocol;


enum CapabilityFlags : uint {
	CLIENT_LONG_PASSWORD                = 0x00000001,  // Use the improved version of Old Password Authentication
	CLIENT_FOUND_ROWS                   = 0x00000002,  // Send found rows instead of affected rows in EOF_Packet
	CLIENT_LONG_FLAG                    = 0x00000004,  // Longer flags in Protocol::ColumnDefinition320
	CLIENT_CONNECT_WITH_DB              = 0x00000008,  // One can specify db on connect in Handshake Response Packet
	CLIENT_NO_SCHEMA                    = 0x00000010,  // Don't allow database.table.column
	CLIENT_COMPRESS                     = 0x00000020,  // Compression protocol supported
	CLIENT_ODBC                         = 0x00000040,  // Special handling of ODBC behaviour
	CLIENT_LOCAL_FILES                  = 0x00000080,  // Can use LOAD DATA LOCAL
	CLIENT_IGNORE_SPACE                 = 0x00000100,  // Parser can ignore spaces before '('
	CLIENT_PROTOCOL_41                  = 0x00000200,  // Supports the 4.1 protocol
	CLIENT_INTERACTIVE                  = 0x00000400,  // wait_timeout vs. wait_interactive_timeout
	CLIENT_SSL                          = 0x00000800,  // Supports SSL
	CLIENT_IGNORE_SIGPIPE               = 0x00001000,  // Don't issue SIGPIPE if network failures (libmysqlclient only)
	CLIENT_TRANSACTIONS                 = 0x00002000,  // Can send status flags in EOF_Packet
	CLIENT_RESERVED                     = 0x00004000,  // Unused
	CLIENT_SECURE_CONNECTION            = 0x00008000,  // Supports Authentication::Native41
	CLIENT_MULTI_STATEMENTS             = 0x00010000,  // Can handle multiple statements per COM_QUERY and COM_STMT_PREPARE
	CLIENT_MULTI_RESULTS                = 0x00020000,  // Can send multiple resultsets for COM_QUERY
	CLIENT_PS_MULTI_RESULTS             = 0x00040000,  // Can send multiple resultsets for COM_STMT_EXECUTE
	CLIENT_PLUGIN_AUTH                  = 0x00080000,  // Sends extra data in Initial Handshake Packet and supports the pluggable authentication protocol.
	CLIENT_CONNECT_ATTRS                = 0x00100000,  // Allows connection attributes in Protocol::HandshakeResponse41
	CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000,  // Understands length encoded integer for auth response data in Protocol::HandshakeResponse41
	CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS = 0x00400000,  // Announces support for expired password extension
	CLIENT_SESSION_TRACK                = 0x00800000,  // Can set SERVER_SESSION_STATE_CHANGED in the Status Flags and send session-state change data after a OK packet
	CLIENT_DEPRECATE_EOF                = 0x01000000,  // Can send OK after a Text Resultset
}


enum StatusFlags : ushort {
	SERVER_STATUS_IN_TRANS	            = 0x0001,  // A transaction is active
	SERVER_STATUS_AUTOCOMMIT	        = 0x0002,  // auto-commit is enabled
	SERVER_MORE_RESULTS_EXISTS	        = 0x0008,
	SERVER_STATUS_NO_GOOD_INDEX_USED    = 0x0010,
	SERVER_STATUS_NO_INDEX_USED	        = 0x0020,
	SERVER_STATUS_CURSOR_EXISTS	        = 0x0040,  // Used by Binary Protocol Resultset to signal that COM_STMT_FETCH has to be used to fetch the row-data.
	SERVER_STATUS_LAST_ROW_SENT	        = 0x0080,
	SERVER_STATUS_DB_DROPPED	        = 0x0100,
	SERVER_STATUS_NO_BACKSLASH_ESCAPES	= 0x0200,
	SERVER_STATUS_METADATA_CHANGED	    = 0x0400,
	SERVER_QUERY_WAS_SLOW	            = 0x0800,
	SERVER_PS_OUT_PARAMS	            = 0x1000,
	SERVER_STATUS_IN_TRANS_READONLY	    = 0x2000,  // In a read-only transaction
	SERVER_SESSION_STATE_CHANGED	    = 0x4000,  // connection state information has changed
}


enum StatusPackets : ubyte {
	OK_Packet   = 0,
	ERR_Packet  = 0xff,
	EOF_Packet  = 0xfe,
}


enum Commands : ubyte {
	//COM_SLEEP           = 0x00,
	COM_QUIT            = 0x01,
	COM_INIT_DB         = 0x02,
	COM_QUERY           = 0x03,
	COM_FIELD_LIST      = 0x04,
	COM_CREATE_DB       = 0x05,
	COM_DROP_DB         = 0x06,
	COM_REFRESH         = 0x07,
	//COM_SHUTDOWN        = 0x08,
	COM_STATISTICS      = 0x09,
	COM_PROCESS_INFO    = 0x0a,
	//COM_CONNECT         = 0x0b,
	COM_PROCESS_KILL    = 0x0c,
	COM_DEBUG           = 0x0d,
	COM_PING            = 0x0e,
	//COM_TIME            = 0x0f,
	//COM_DELAYED_INSERT  = 0x10,
	COM_CHANGE_USER     = 0x11,
	COM_BINLOG_DUMP     = 0x12,
	COM_TABLE_DUMP      = 0x13,
	//COM_CONNECT_OUT     = 0x14,
	COM_REGISTER_SLAVE  = 0x15,
	COM_STMT_PREPARE    = 0x16,
	COM_STMT_EXECUTE    = 0x17,
	COM_STMT_SEND_LONG_DATA = 0x18,
	COM_STMT_CLOSE      = 0x19,
	COM_STMT_RESET      = 0x1a,
	COM_SET_OPTION      = 0x1b,
	COM_STMT_FETCH      = 0x1c,
	//COM_DAEMON          = 0x1d,
	COM_BINLOG_DUMP_GTID = 0x1e,
	COM_RESET_CONNECTION = 0x1f,
}


enum Cursors : ubyte {
	CURSOR_TYPE_NO_CURSOR   = 0x00,
	CURSOR_TYPE_READ_ONLY   = 0x01,
	CURSOR_TYPE_FOR_UPDATE  = 0x02,
	CURSOR_TYPE_SCROLLABLE  = 0x04,
}


enum ColumnTypes : ubyte {
	MYSQL_TYPE_DECIMAL      = 0x00,
	MYSQL_TYPE_TINY         = 0x01,
	MYSQL_TYPE_SHORT        = 0x02,
	MYSQL_TYPE_LONG	        = 0x03,
	MYSQL_TYPE_FLOAT	    = 0x04,
	MYSQL_TYPE_DOUBLE	    = 0x05,
	MYSQL_TYPE_NULL	        = 0x06,
	MYSQL_TYPE_TIMESTAMP	= 0x07,
	MYSQL_TYPE_LONGLONG	    = 0x08,
	MYSQL_TYPE_INT24	    = 0x09,
	MYSQL_TYPE_DATE	        = 0x0a,
	MYSQL_TYPE_TIME	        = 0x0b,
	MYSQL_TYPE_DATETIME	    = 0x0c,
	MYSQL_TYPE_YEAR	        = 0x0d,
	MYSQL_TYPE_NEWDATE  	= 0x0e,
	MYSQL_TYPE_VARCHAR	    = 0x0f,
	MYSQL_TYPE_BIT	        = 0x10,
	MYSQL_TYPE_TIMESTAMP2   = 0x11,
	MYSQL_TYPE_DATETIME2    = 0x12,
	MYSQL_TYPE_TIME2        = 0x13,
	MYSQL_TYPE_JSON         = 0xf5,
	MYSQL_TYPE_NEWDECIMAL   = 0xf6,
	MYSQL_TYPE_ENUM         = 0xf7,
	MYSQL_TYPE_SET	        = 0xf8,
	MYSQL_TYPE_TINY_BLOB	= 0xf9,
	MYSQL_TYPE_MEDIUM_BLOB	= 0xfa,
	MYSQL_TYPE_LONG_BLOB	= 0xfb,
	MYSQL_TYPE_BLOB	        = 0xfc,
	MYSQL_TYPE_VAR_STRING	= 0xfd,
	MYSQL_TYPE_STRING	    = 0xfe,
	MYSQL_TYPE_GEOMETRY	    = 0xff,
}


auto columnTypeName(ColumnTypes type) {
	final switch (type) with (ColumnTypes) {
	case MYSQL_TYPE_DECIMAL:	return "decimal";
	case MYSQL_TYPE_TINY:		return "tiny";
	case MYSQL_TYPE_SHORT:		return "short";
	case MYSQL_TYPE_LONG:		return "long";
	case MYSQL_TYPE_FLOAT:		return "float";
	case MYSQL_TYPE_DOUBLE:		return "double";
	case MYSQL_TYPE_NULL:		return "null";
	case MYSQL_TYPE_TIMESTAMP:	return "timestamp";
	case MYSQL_TYPE_LONGLONG:	return "longlong";
	case MYSQL_TYPE_INT24:		return "int24";
	case MYSQL_TYPE_DATE:		return "date";
	case MYSQL_TYPE_TIME:		return "time";
	case MYSQL_TYPE_DATETIME:	return "datetime";
	case MYSQL_TYPE_YEAR:		return "year";
	case MYSQL_TYPE_NEWDATE:	return "newdate";
	case MYSQL_TYPE_VARCHAR:	return "varchar";
	case MYSQL_TYPE_BIT:		return "bit";
	case MYSQL_TYPE_TIMESTAMP2:	return "timestamp2";
	case MYSQL_TYPE_DATETIME2:	return "datetime2";
	case MYSQL_TYPE_TIME2:		return "time2";
	case MYSQL_TYPE_JSON:		return "json";
	case MYSQL_TYPE_NEWDECIMAL:	return "newdecimal";
	case MYSQL_TYPE_ENUM:		return "enum";
	case MYSQL_TYPE_SET:		return "set";
	case MYSQL_TYPE_TINY_BLOB:	return "tiny_blob";
	case MYSQL_TYPE_MEDIUM_BLOB:return "medium_blob";
	case MYSQL_TYPE_LONG_BLOB:	return "long_blob";
	case MYSQL_TYPE_BLOB:		return "blob";
	case MYSQL_TYPE_VAR_STRING:	return "var_string";
	case MYSQL_TYPE_STRING:		return "string";
	case MYSQL_TYPE_GEOMETRY:	return "geometry";
	}
}


enum FieldFlags : ushort {
	NOT_NULL_FLAG           = 0x0001, //  Field cannot be NULL
	PRI_KEY_FLAG	        = 0x0002, //  Field is part of a primary key
	UNIQUE_KEY_FLAG	        = 0x0004, //  Field is part of a unique key
	MULTIPLE_KEY_FLAG       = 0x0008, //  Field is part of a nonunique key
	BLOB_FLAG	            = 0x0010, //  Field is a BLOB or TEXT (deprecated)
	UNSIGNED_FLAG	        = 0x0020, //  Field has the UNSIGNED attribute
	ZEROFILL_FLAG	        = 0x0040, //  Field has the ZEROFILL attribute
	BINARY_FLAG	            = 0x0080, //  Field has the BINARY attribute
	ENUM_FLAG	            = 0x0100, //  Field is an ENUM
	AUTO_INCREMENT_FLAG	    = 0x0200, //  Field has the AUTO_INCREMENT attribute
	TIMESTAMP_FLAG	        = 0x0400, //  Field is a TIMESTAMP (deprecated)
	SET_FLAG	            = 0x0800, //  Field is a SET
	NO_DEFAULT_VALUE_FLAG   = 0x1000, //  Field has no default value; see additional notes following table
	ON_UPDATE_NOW_FLAG      = 0x2000, // Field is set to NOW on UPDATE
//    PART_KEY_FLAG           = 0x4000, //  Intern; Part of some key
	NUM_FLAG	            = 0x8000, //  Field is numeric
}


enum ErrorCodes : ushort {
	ER_DUP_KEYNAME                  = 1061,
	ER_DUP_ENTRY                    = 1062,
	ER_DUP_ENTRY_WITH_KEY_NAME      = 1586,
}
