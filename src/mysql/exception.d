module mysql.exception;


class MySQLException : Exception {
	this(string msg, string file = __FILE__, size_t line = __LINE__) pure {
		super(msg, file, line);
	}
}

class MySQLConnectionException: MySQLException {
	this(string msg, string file = __FILE__, size_t line = __LINE__) pure {
		super(msg, file, line);
	}
}

class MySQLProtocolException: MySQLException {
	this(string msg, string file = __FILE__, size_t line = __LINE__) pure {
		super(msg, file, line);
	}
}

class MySQLErrorException : Exception {
	this(string msg, string file = __FILE__, size_t line = __LINE__) pure {
		super(msg, file, line);
	}
}

class MySQLDuplicateEntryException : MySQLErrorException {
	this(string msg, string file = __FILE__, size_t line = __LINE__) pure {
		super(msg, file, line);
	}
}
