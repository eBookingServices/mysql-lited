module mysql.connection;


import std.algorithm;
import std.array;
import std.string;
import std.traits;

public import mysql.exception;
import mysql.packet;
import mysql.protocol;
public import mysql.type;


immutable CapabilityFlags DefaultClientCaps = CapabilityFlags.CLIENT_LONG_PASSWORD | CapabilityFlags.CLIENT_LONG_FLAG |
	CapabilityFlags.CLIENT_CONNECT_WITH_DB | CapabilityFlags.CLIENT_PROTOCOL_41 | CapabilityFlags.CLIENT_SECURE_CONNECTION;


struct ConnectionStatus {
	ulong affected = 0;
	ulong insertID = 0;
	ushort flags = 0;
	ushort error = 0;
	ushort warnings = 0;
}


private struct ConnectionSettings {
	CapabilityFlags caps = DefaultClientCaps;

	const(char)[] host;
	const(char)[] user;
	const(char)[] pwd;
	const(char)[] db;
	ushort port = 3306;
}


private struct ServerInfo {
	const(char)[] versionString;
	ubyte protocol;
	ubyte charSet;
	ushort status;
	uint connection;
	uint caps;
}


@property string placeholders(size_t x, bool parens = true) {
	import std.range : repeat, take;

	if (parens)
		return "(" ~ ("?".repeat().take(x).join(",")) ~ ")";
	return "?".repeat.take(x).join(",");
}


@property string placeholders(T)(T[] x, bool parens = true) {
	import std.range : repeat, take;

	if (parens)
		return "(" ~ ("?".repeat().take(x.length).join(",")) ~ ")";
	return "?".repeat.take(x.length).join(",");
}


struct PreparedStatement {
package:
	uint id;
	uint params;
}


struct Connection(SocketType) {
	void connect(string connectionString) {
		connectionSettings(connectionString);
		connect();
	}

	void connect(const(char)[] host, ushort port, const(char)[] user, const(char)[] pwd, const(char)[] db, CapabilityFlags caps = DefaultClientCaps) {
		settings_.host = host;
		settings_.user = user;
		settings_.pwd = pwd;
		settings_.db = db;
		settings_.port = port;
		settings_.caps = caps | CapabilityFlags.CLIENT_LONG_PASSWORD | CapabilityFlags.CLIENT_PROTOCOL_41;

		connect();
	}

	void use(const(char)[] db) {
		send(Commands.COM_INIT_DB, db);
		eatStatus(retrieve());
	}

	void ping() {
		send(Commands.COM_PING);
		eatStatus(retrieve());
	}

	void refresh() {
		send(Commands.COM_REFRESH);
		eatStatus(retrieve());
	}

	void reset() {
		send(Commands.COM_RESET_CONNECTION);
		eatStatus(retrieve());
	}

	const(char)[] statistics() {
		send(Commands.COM_STATISTICS);

		auto answer = retrieve();
		return answer.eat!(const(char)[])(answer.remaining);
	}

	auto prepare(const(char)[] sql) {
		send(Commands.COM_STMT_PREPARE, sql);

		auto answer = retrieve();

		if (answer.peek!ubyte != StatusPackets.OK_Packet)
			check(answer);

		answer.expect!ubyte(0);

		auto id = answer.eat!uint;
		auto columns = answer.eat!ushort;
		auto params = answer.eat!ushort;
		answer.expect!ubyte(0);

		auto warnings = answer.eat!ushort;

		if (params) {
			foreach (i; 0..params)
				skipColumnDef(retrieve(), Commands.COM_STMT_PREPARE);

			eatEOF(retrieve());
		}

		if (columns) {
			MySQLColumn def;
			foreach (i; 0..columns)
				skipColumnDef(retrieve(), Commands.COM_STMT_PREPARE);

			eatEOF(retrieve());
		}

		return PreparedStatement(id, params);
	}

	void execute(Args...)(const(char)[] stmt, Args args) {
		scope(failure) disconnect();

		auto id = prepare(stmt);
		execute(id, args);
		close(id);
	}

	void begin() {
		if (status_.flags & StatusFlags.SERVER_STATUS_IN_TRANS)
			throw new MySQLErrorException("MySQL does not support nested transactions - commit or rollback before starting a new transaction");

		query("start transaction");

		assert(status_.flags & StatusFlags.SERVER_STATUS_IN_TRANS);
	}

	void commit() {
		if ((status_.flags & StatusFlags.SERVER_STATUS_IN_TRANS) == 0)
			throw new MySQLErrorException("No active transaction");

		query("commit");

		assert((status_.flags & StatusFlags.SERVER_STATUS_IN_TRANS) == 0);
	}

	void rollback() {
		if (connected) {
			if ((status_.flags & StatusFlags.SERVER_STATUS_IN_TRANS) == 0)
				throw new MySQLErrorException("No active transaction");

			query("rollback");

			assert((status_.flags & StatusFlags.SERVER_STATUS_IN_TRANS) == 0);
		}
	}

	@property bool inTransaction() const {
		return connected && (status_.flags & StatusFlags.SERVER_STATUS_IN_TRANS);
	}

	void execute(Args...)(PreparedStatement stmt, Args args) {
		scope(failure) disconnect();

		ensureConnected();

		seq_ = 0;
		auto packet = OutputPacket(&out_);
		packet.put!ubyte(Commands.COM_STMT_EXECUTE);
		packet.put!uint(stmt.id);
		packet.put!ubyte(Cursors.CURSOR_TYPE_READ_ONLY);
		packet.put!uint(1);

		static if (args.length == 0) {
			enum shouldDiscard = true;
		} else {
			enum shouldDiscard = !isCallable!(args[args.length - 1]);
		}

		enum argCount = shouldDiscard ? args.length : (args.length - 1);

		if (!argCount && stmt.params)
			throw new MySQLErrorException(format("Wrong number of parameters for query. Got 0 but expected %d.", stmt.params));

		static if (argCount) {
		enum NullsCapacity = 128; // must be power of 2
		ubyte[NullsCapacity >> 3] nulls;
		size_t bitsOut = 0;
		size_t indexArg = 0;
			foreach(i, arg; args[0..argCount]) {
			const auto index = (indexArg >> 3) & (NullsCapacity - 1);
			const auto bit = indexArg & 7;

			static if (is(typeof(arg) == typeof(null))) {
				nulls[index] = nulls[index] | (1 << bit);
				++indexArg;
			} else static if (is(Unqual!(typeof(arg)) == MySQLValue)) {
				if (arg.isNull)
					nulls[index] = nulls[index] | (1 << bit);
				++indexArg;
			} else static if (isArray!(typeof(arg)) && !isSomeString!(typeof(arg))) {
				indexArg += arg.length;
			} else {
				++indexArg;
			}

				auto finishing = (i == argCount - 1);
			auto remaining = indexArg - bitsOut;

			if (finishing || (remaining >= NullsCapacity)) {
				while (remaining) {
					auto bits = min(remaining, NullsCapacity);

					packet.put(nulls[0..(bits + 7) >> 3]);
					bitsOut += bits;
					nulls[] = 0;

					remaining = (indexArg - bitsOut);
					if (!remaining || (!finishing && (remaining < NullsCapacity)))
						break;
				}
			}
		}
		packet.put!ubyte(1);

			if (indexArg != stmt.params)
				throw new MySQLErrorException(format("Wrong number of parameters for query. Got %d but expected %d.", indexArg, stmt.params));

			foreach (arg; args[0..argCount])
			putValueType(packet, arg);

			foreach (arg; args[0..argCount]) {
			static if (!is(typeof(arg) == typeof(null))) {
				putValue(packet, arg);
			}
		}
	}

		packet.finalize(seq_);
		++seq_;

		socket_.write(packet.get());

		auto answer = retrieve();
		if (isStatus(answer)) {
			eatStatus(answer);
		} else {
			static if (!shouldDiscard) {
				resultSet(answer, stmt.id, Commands.COM_STMT_EXECUTE, args[args.length - 1]);
			} else {
				discardAll(answer, Commands.COM_STMT_EXECUTE);
			}
		}
	}

	void close(PreparedStatement stmt) {
		uint[1] data = [ stmt.id ];
		send(Commands.COM_STMT_CLOSE, data);
	}

	alias OnStatusCallback = void delegate(ConnectionStatus status, const(char)[] message);
	@property void onStatus(OnStatusCallback callback) {
		onStatus_ = callback;
	}

	@property OnStatusCallback onStatus() const {
		return onStatus_;
	}

	@property ulong insertID() {
		return status_.insertID;
	}

	@property ulong affected() {
		return cast(size_t)status_.affected;
	}

	@property size_t warnings() {
		return status_.warnings;
	}

	@property size_t error() {
		return status_.error;
	}

	@property const(char)[] status() const {
		return info_;
	}

	@property bool connected() const {
		return socket_.connected;
	}

	void disconnect() {
		socket_.close();
	}

	~this() {
		disconnect();
	}

private:
	void connect() {
		socket_.connect(settings_.host, settings_.port);

		seq_ = 0;
		eatHandshake(retrieve());
	}

	void send(T)(Commands cmd, T[] data) {
		send(cmd, cast(ubyte*)data.ptr, data.length * T.sizeof);
	}

	void send(Commands cmd, ubyte* data = null, size_t length = 0) {
		if(!socket_.connected)
			connect();

		seq_ = 0;
		auto header = OutputPacket(&out_);
		header.put!ubyte(cmd);
		header.finalize(seq_, length);
		++seq_;

		socket_.write(header.get());
		if (length)
			socket_.write(data[0..length]);
	}

	void query(const(char)[] sql) {
		send(Commands.COM_QUERY, sql);

		auto answer = retrieve();
		if (isStatus(answer))
			eatStatus(answer);
	}

	void ensureConnected() {
		if(!socket_.connected)
			connect();
	}

	bool isStatus(InputPacket packet) {
		auto id = packet.peek!ubyte;
		switch (id) {
			case StatusPackets.ERR_Packet:
			case StatusPackets.OK_Packet:
				return 1;
			default:
				return false;
		}
	}

	void check(InputPacket packet, bool smallError = false) {
		auto id = packet.peek!ubyte;
		switch (id) {
			case StatusPackets.ERR_Packet:
			case StatusPackets.OK_Packet:
				eatStatus(packet, smallError);
				break;
			default:
				break;
		}
	}

	InputPacket retrieve() {
		scope(failure) disconnect();

		ubyte[4] header;
		socket_.read(header);

		auto len = header[0] | (header[1] << 8) | (header[2] << 16);
		auto seq = header[3];

		if (seq != seq_)
			throw new MySQLConnectionException("Out of order packet received");

		++seq_;

		in_.length = len;
		socket_.read(in_);

		if (in_.length != len)
			throw new MySQLConnectionException("Wrong number of bytes read");

		return InputPacket(&in_);
	}

	void eatHandshake(InputPacket packet) {
		scope(failure) disconnect();

		check(packet, true);

		server_.protocol = packet.eat!ubyte;
		server_.versionString = packet.eat!(const(char)[])(packet.countUntil(0, true));
		packet.skip(1);

		server_.connection = packet.eat!uint;

		const auto authLengthStart = 8;
		size_t authLength = authLengthStart;

		ubyte[256] auth;
		auth[0..authLength] = packet.eat!(ubyte[])(authLength);

		packet.expect!ubyte(0);

		server_.caps = packet.eat!ushort;

		if (!packet.empty) {
			server_.charSet = packet.eat!ubyte;
			server_.status = packet.eat!ushort;
			server_.caps |= packet.eat!ushort << 16;
			server_.caps |= CapabilityFlags.CLIENT_LONG_PASSWORD;

			if ((server_.caps & CapabilityFlags.CLIENT_PROTOCOL_41) == 0)
				throw new MySQLProtocolException("Server doesn't support protocol v4.1");

			if (server_.caps & CapabilityFlags.CLIENT_SECURE_CONNECTION) {
				packet.skip(1);
			} else {
				packet.expect!ubyte(0);
			}

			packet.skip(10);

			authLength += packet.countUntil(0, true);
			if (authLength > auth.length)
				throw new MySQLConnectionException("Bad packet format");

			auth[authLengthStart..authLength] = packet.eat!(ubyte[])(authLength - authLengthStart);

			packet.expect!ubyte(0);
		}

		ubyte[20] token;
		{
			import std.digest.sha;

			auto pass = sha1Of(cast(const(ubyte)[])settings_.pwd);
			token = sha1Of(pass);

			SHA1 sha1;
			sha1.start();
			sha1.put(auth[0..authLength]);
			sha1.put(token);
			token = sha1.finish();

			foreach (i; 0..20)
				token[i] = token[i] ^ pass[i];
		}

		caps_ = cast(CapabilityFlags)(settings_.caps & server_.caps);

		auto reply = OutputPacket(&out_);
		reply.reserve(64 + settings_.user.length + settings_.pwd.length + settings_.db.length);

		reply.put!uint(caps_);
		reply.put!uint(1);
		reply.put!ubyte(33);
		reply.fill(0, 23);

		reply.put(settings_.user);
		reply.put!ubyte(0);

		if (settings_.pwd.length) {
			if (caps_ & CapabilityFlags.CLIENT_SECURE_CONNECTION) {
				reply.put!ubyte(token.length);
				reply.put(token);
			} else {
				reply.put(token);
				reply.put!ubyte(0);
			}
		} else {
			reply.put!ubyte(0);
		}

		if (settings_.db.length && (caps_ & CapabilityFlags.CLIENT_CONNECT_WITH_DB))
			reply.put(settings_.db);

		reply.put!ubyte(0);

		reply.finalize(seq_);
		++seq_;

		socket_.write(reply.get());

		eatStatus(retrieve());
	}

	void eatStatus(InputPacket packet, bool smallError = false) {
		auto id = packet.eat!ubyte;

		switch (id) {
		case StatusPackets.OK_Packet:
			status_.error = 0;
			status_.affected = packet.eatLenEnc();
			status_.insertID = packet.eatLenEnc();
			status_.flags = packet.eat!ushort;
			status_.warnings = packet.eat!ushort;

			if (caps_ & CapabilityFlags.CLIENT_SESSION_TRACK) {
				info(packet.eat!(const(char)[])(cast(size_t)packet.eatLenEnc()));
				packet.skip(1);

				if (status_.flags & StatusFlags.SERVER_SESSION_STATE_CHANGED) {
					packet.skip(cast(size_t)packet.eatLenEnc());
					packet.skip(1);
				}
			} else if (!packet.empty) {
				auto len = cast(size_t)packet.eatLenEnc();
				info(packet.eat!(const(char)[])(min(len, packet.remaining)));
			}

			if (onStatus_)
				onStatus_(status_, info_);

			break;
		case StatusPackets.EOF_Packet:
			status_.error = 0;
			status_.warnings = packet.eat!ushort;
			status_.flags = packet.eat!ushort;
			info([]);

			if (onStatus_)
				onStatus_(status_, info_);

			break;
		case StatusPackets.ERR_Packet:
			status_.flags = 0;
			status_.warnings = 0;
			status_.error = packet.eat!ushort;
			if (!smallError)
				packet.skip(6);
			info(packet.eat!(const(char)[])(packet.remaining));

			if (onStatus_)
				onStatus_(status_, info_);

			switch(status_.error) {
			case ErrorCodes.ER_DUP_ENTRY_WITH_KEY_NAME:
			case ErrorCodes.ER_DUP_ENTRY:
				throw new MySQLDuplicateEntryException(cast(string)info_);
			default:
				throw new MySQLErrorException(cast(string)info_);
			}
		default:
			throw new MySQLProtocolException("Unexpected packet format");
		}
	}

	void info(const(char)[] value) {
		info_.length = value.length;
		info_[0..$] = value;
	}

	void skipColumnDef(InputPacket packet, Commands cmd) {
		packet.skip(cast(size_t)packet.eatLenEnc());	// catalog
		packet.skip(cast(size_t)packet.eatLenEnc());	// schema
		packet.skip(cast(size_t)packet.eatLenEnc());	// table
		packet.skip(cast(size_t)packet.eatLenEnc());	// original_table
		packet.skip(cast(size_t)packet.eatLenEnc());	// name
		packet.skip(cast(size_t)packet.eatLenEnc());	// original_name
		packet.skipLenEnc();							// next_length
		packet.skip(10); // 2 + 4 + 1 + 2 + 1			// charset, length, type, flags, decimals
		packet.expect!ushort(0);

		if (cmd == Commands.COM_FIELD_LIST)
			packet.skip(cast(size_t)packet.eatLenEnc());// default values
	}

	void columnDef(InputPacket packet, Commands cmd, ref MySQLColumn def) {
		packet.skip(cast(size_t)packet.eatLenEnc());	// catalog
		packet.skip(cast(size_t)packet.eatLenEnc());	// schema
		packet.skip(cast(size_t)packet.eatLenEnc());	// table
		packet.skip(cast(size_t)packet.eatLenEnc());	// original_table
		auto len = cast(size_t)packet.eatLenEnc();
		columns_ ~= packet.eat!(const(char)[])(len);
		def.name = columns_[$-len..$];
		packet.skip(cast(size_t)packet.eatLenEnc());	// original_name
		packet.skipLenEnc();							// next_length
		packet.skip(2);									// charset
		def.length = packet.eat!uint;
		def.type = cast(ColumnTypes)packet.eat!ubyte;
		def.flags = packet.eat!ushort;
		def.decimals = packet.eat!ubyte;

		packet.expect!ushort(0);

		if (cmd == Commands.COM_FIELD_LIST)
			packet.skip(cast(size_t)packet.eatLenEnc());// default values
	}

	void columnDefs(size_t count, Commands cmd, ref MySQLColumn[] defs) {
		defs.length = count;
		foreach (i; 0..count)
			columnDef(retrieve(), cmd, defs[i]);
	}

	void resultSetRow(InputPacket packet, Commands cmd, MySQLHeader header, MySQLRow row) {
		assert(row.length == header.length);

		packet.expect!ubyte(0);
		auto nulls = packet.eat!(ubyte[])((header.length + 2 + 7) >> 3);
		foreach (i, column; header) {
			const auto index = (i + 2) >> 3; // bit offset of 2
			const auto bit = (i + 2) & 7;

			if ((nulls[index] & (1 << bit)) == 0) {
				row.set(i, eatValue(packet, column));
			} else {
				row.nullify(i);
			}
		}
		assert(packet.empty);
	}

	bool callHandler(RowHandler)(RowHandler handler, size_t i, MySQLHeader header, MySQLRow row) if ((ParameterTypeTuple!(RowHandler).length == 1) && is(ParameterTypeTuple!(RowHandler)[0] == MySQLRow)) {
		static if (is(ReturnType!(RowHandler) == void)) {
			handler(row);
			return true;
		} else {
			return handler(row); // return type must be bool
		}
	}

	bool callHandler(RowHandler)(RowHandler handler, size_t i, MySQLHeader header, MySQLRow row) if ((ParameterTypeTuple!(RowHandler).length == 2) && isNumeric!(ParameterTypeTuple!(RowHandler)[0]) && is(ParameterTypeTuple!(RowHandler)[1] == MySQLRow)) {
		static if (is(ReturnType!(RowHandler) == void)) {
			handler(cast(ParameterTypeTuple!(RowHandler)[0])i, row);
			return true;
		} else {
			return handler(cast(ParameterTypeTuple!(RowHandler)[0])i, row); // return type must be bool
		}
	}

	bool callHandler(RowHandler)(RowHandler handler, size_t i, MySQLHeader header, MySQLRow row) if ((ParameterTypeTuple!(RowHandler).length == 2) && is(ParameterTypeTuple!(RowHandler)[0] == MySQLHeader) && is(ParameterTypeTuple!(RowHandler)[1] == MySQLRow)) {
		static if (is(ReturnType!(RowHandler) == void)) {
			handler(header, row);
			return true;
		} else {
			return handler(header, row); // return type must be bool
		}
	}

	bool callHandler(RowHandler)(RowHandler handler, size_t i, MySQLHeader header, MySQLRow row) if ((ParameterTypeTuple!(RowHandler).length == 3) && isNumeric!(ParameterTypeTuple!(RowHandler)[0]) && is(ParameterTypeTuple!(RowHandler)[1] == MySQLHeader) && is(ParameterTypeTuple!(RowHandler)[2] == MySQLRow)) {
		static if (is(ReturnType!(RowHandler) == void)) {
			handler(i, header, row);
			return true;
		} else {
			return handler(i, header, row); // return type must be bool
		}
	}

	void resultSet(RowHandler)(InputPacket packet, uint stmt, Commands cmd, RowHandler handler) {
		columns_.length = 0;

		auto columns = cast(size_t)packet.eatLenEnc();
		columnDefs(columns, cmd, header_);
		row_.length = columns;
		row_.header(header_);

		size_t index = 0;
		auto statusFlags = eatEOF(retrieve());
		if (statusFlags & StatusFlags.SERVER_STATUS_CURSOR_EXISTS) {
			uint[2] data = [ stmt, 4096 ]; // todo: make setting - rows per fetch
			while (statusFlags & (StatusFlags.SERVER_STATUS_CURSOR_EXISTS | StatusFlags.SERVER_MORE_RESULTS_EXISTS)) {
				send(Commands.COM_STMT_FETCH, data);

				auto answer = retrieve();
				if (answer.peek!ubyte == StatusPackets.ERR_Packet)
					check(answer);

				auto row = answer.empty ? retrieve() : answer;
				while (true) {
					if (row.peek!ubyte == StatusPackets.EOF_Packet) {
						statusFlags = eatEOF(row);
						break;
					}

					resultSetRow(row, Commands.COM_STMT_FETCH, header_, row_);
					if (!callHandler(handler, index++, header_, row_)) {
						discardUntilEOF(retrieve());
						statusFlags = 0;
						break;
					}
					row = retrieve();
				}
			}
		} else {
			auto row = retrieve();
			while (true) {
				if (row.peek!ubyte == StatusPackets.EOF_Packet) {
					eatEOF(row);
					break;
				}

				resultSetRow(row, cmd, header_, row_);
				if (!callHandler(handler, index++, header_, row_)) {
					discardUntilEOF(retrieve());
					break;
				}

				row = retrieve();
			}
		}
	}

	void discardAll(InputPacket packet, Commands cmd) {
		auto columns = cast(size_t)packet.eatLenEnc();
		columnDefs(columns, cmd, header_);

		auto statusFlags = eatEOF(retrieve());
		if ((statusFlags & StatusFlags.SERVER_STATUS_CURSOR_EXISTS) == 0) {
			while (true) {
				auto row = retrieve();
				if (row.peek!ubyte == StatusPackets.EOF_Packet) {
					eatEOF(row);
					break;
				}
			}
		}
	}

	void discardUntilEOF(InputPacket packet) {
		while (true) {
			if (packet.peek!ubyte == StatusPackets.EOF_Packet) {
				eatEOF(packet);
				break;
			}
			packet = retrieve();
		}
	}

	auto eatEOF(InputPacket packet) {
		auto id = packet.eat!ubyte;
		if (id != StatusPackets.EOF_Packet)
			throw new MySQLProtocolException("Unexpected packet format");

		status_.error = 0;
		status_.warnings = packet.eat!ushort();
		status_.flags = packet.eat!ushort();
		info([]);

		if (onStatus_)
			onStatus_(status_, info_);

		return status_.flags;
	}

	void connectionSettings(const(char)[] connectionString) {
		import std.conv;

		auto remaining = connectionString;

		auto indexValue = remaining.indexOf("=");
		while (!remaining.empty) {
			auto indexValueEnd = remaining.indexOf(";", indexValue);
			if (indexValueEnd <= 0)
				indexValueEnd = remaining.length;

			auto name = strip(remaining[0..indexValue]);
			auto value = strip(remaining[indexValue+1..indexValueEnd]);

			switch (name) {
			case "host":
				settings_.host = value;
				break;
			case "user":
				settings_.user = value;
				break;
			case "pwd":
				settings_.pwd = value;
				break;
			case "db":
				settings_.db = value;
				break;
			case "port":
				settings_.port = to!ushort(value);
				break;
			default:
				throw new MySQLException("Bad connection string: " ~ cast(string)connectionString);
			}

			if (indexValueEnd == remaining.length)
				return;

			remaining = remaining[indexValueEnd+1..$];
			indexValue = remaining.indexOf("=");
		}

		throw new MySQLException("Bad connection string: " ~ cast(string)connectionString);
	}

	SocketType socket_;
	MySQLHeader header_;
	MySQLRow row_;
	char[] columns_;
	char[] info_;
	ubyte[] in_;
	ubyte[] out_;
	ubyte seq_ = 0;

	OnStatusCallback onStatus_;
	CapabilityFlags caps_;
	ConnectionStatus status_;
	ConnectionSettings settings_;
	ServerInfo server_;
}
