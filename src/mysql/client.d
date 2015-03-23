module mysql.client;

import vibe.core.connectionpool;

public import mysql.connection;
import mysql.socket;


final class MySQLClientT(SocketType) {
	this(string connectionString) {
		connections_ = new ConnectionPool!(Connection!SocketType*)({
			auto ret = new Connection!SocketType();
			ret.connect(connectionString);
			return ret;
		});

		lockConnection();
	}

	this(string host, ushort port, string user, string pwd, string db) {
		connections_ = new ConnectionPool!ConnectionType({
			auto ret = new Connection!SocketType();
			ret.connect(host, port, user, pwd, db);
			return ret;
		});

		lockConnection();
	}

    auto lockConnection() {
        auto connection = connections_.lockConnection();
        if (connection.inTransaction)
            connection.rollback();
        connection.onStatus = null;
        return connection;
    }

    package alias ConnectionType = Connection!VibeSocket*;
    private ConnectionPool!(Connection!SocketType*) connections_;
}

alias MySQLClient = MySQLClientT!VibeSocket;