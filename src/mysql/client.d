module mysql.client;

import vibe.core.connectionpool;

public import mysql.connection;
import mysql.socket;


final class MySQLClientT(SocketType, ConnectionOptions Options = ConnectionOptions.Default) {
	this(string connectionString) {
		connections_ = new ConnectionPoolType({
			auto ret = new ConnectionType();
			ret.connect(connectionString);
			return ret;
		});
	}

	this(string host, ushort port, string user, string pwd, string db) {
		connections_ = new ConnectionPoolType({
			auto ret = new ConnectionType();
			ret.connect(host, port, user, pwd, db);
			return ret;
		});
	}

	auto lockConnection() {
		auto connection = connections_.lockConnection();
		if (connection.inTransaction)
			connection.rollback();
		connection.onStatus = null;
		return connection;
	}

	package alias ConnectionType = Connection!(VibeSocket, Options);
	package alias ConnectionPoolType = ConnectionPool!(ConnectionType*);
	private ConnectionPoolType connections_;
}

alias MySQLClient = MySQLClientT!VibeSocket;
