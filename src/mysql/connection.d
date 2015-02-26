module mysql.connection;

import std.array;
import std.functional;
import std.string;
import std.traits;

public import mysql.exception;
import mysql.packet;
import mysql.protocol;
public import mysql.type;


immutable CapabilityFlags DefaultClientCaps = CapabilityFlags.CLIENT_LONG_PASSWORD | CapabilityFlags.CLIENT_LONG_FLAG |
    CapabilityFlags.CLIENT_CONNECT_WITH_DB | CapabilityFlags.CLIENT_PROTOCOL_41 | CapabilityFlags.CLIENT_SECURE_CONNECTION;


struct ConnectionSettings {
    CapabilityFlags caps = DefaultClientCaps;

    const(char)[] host;
    const(char)[] user;
    const(char)[] pwd;
    const(char)[] db;
    ushort port = 3306;
}


struct ConnectionStatus {
    CapabilityFlags caps = cast(CapabilityFlags)0;

    ulong affected = 0;
    ulong insertID = 0;
    ushort flags = 0;
    ushort error = 0;
    ushort warnings = 0;
}


struct ServerInfo {
    const(char)[] versionString;
    ubyte protocol;
    ubyte charSet;
    ushort status;
    uint connection;
    uint caps;
}


struct PreparedStatement {
package:
    uint id;    // todo: investigate if it's really necessary to close statements explicitly
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
        status(retrieve());
    }

    void ping() {
        send(Commands.COM_PING);
        status(retrieve());
    }

    void refresh() {
        send(Commands.COM_REFRESH);
        status(retrieve());
    }

    void reset() {
        send(Commands.COM_RESET_CONNECTION);
        status(retrieve());
    }

    const(char)[] statistics() {
        send(Commands.COM_STATISTICS);
        
        auto answer = retrieve();
        return answer.eat!(const(char)[])(answer.remaining);
    }

    auto prepare(const(char)[] sql) {
        send(Commands.COM_STMT_PREPARE, sql);

        auto answer = retrieve();
        check(answer);

        answer.expect!ubyte(0);
        
        auto id = answer.eat!uint;
        auto columns = answer.eat!ushort;
        auto params = answer.eat!ushort;
        answer.expect!ubyte(0);

        auto warnings = answer.eat!ushort;

        if (params) {
            MySQLColumn def;
            foreach (i; 0..params)
                columnDef(retrieve(), Commands.COM_STMT_PREPARE, def);

            skipEOF(retrieve());
        }

        if (columns) {
            MySQLColumn def;
            foreach (i; 0..columns)
                columnDef(retrieve(), Commands.COM_STMT_PREPARE, def);

            skipEOF(retrieve());
        }

        return PreparedStatement(id, params);
    }

    void execute(Args...)(const(char)[] stmt, Args args) {
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
        if (status_.flags & StatusFlags.SERVER_STATUS_IN_TRANS)
            throw new MySQLErrorException("No active transaction");

        query("commit");

        assert((status_.flags & StatusFlags.SERVER_STATUS_IN_TRANS) == 0);
    }

    void rollback() {
        if (status_.flags & StatusFlags.SERVER_STATUS_IN_TRANS)
            throw new MySQLErrorException("No active transaction");

        query("rollback");

        assert((status_.flags & StatusFlags.SERVER_STATUS_IN_TRANS) == 0);
    }

    bool inTransaction() const {
        return (status_.flags & StatusFlags.SERVER_STATUS_IN_TRANS);
    }

    void execute(Args...)(PreparedStatement stmt, Args args) {
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

        if (argCount != stmt.params)
            throw new MySQLErrorException("Wrong number of parameters for query");

        static if (argCount) {
            ubyte[1024] nulls;
            foreach(i, arg; args) {
                const auto index = i >> 3;
                const auto bit = i & 7;

                static if (is(typeof(arg) == typeof(null))) {
                    nulls[index] = nulls[index] | (1 << bit);
                }
            }

            packet.put(nulls[0..((args.length + 7) >> 3)]);
            packet.put!ubyte(1);

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
            status(answer);
        } else {
            static if (!shouldDiscard) {
                resultSet(answer, stmt.id, Commands.COM_STMT_EXECUTE, args[args.length - 1]);
            } else {
                discardAll(answer, Commands.COM_STMT_EXECUTE);
            }
        }
    }

    void query(const(char)[] sql) {
        send(Commands.COM_QUERY, sql);

        auto answer = retrieve();
        if (isStatus(answer))
            status(answer);
    }

    void close(PreparedStatement stmt) {
        uint[1] data = [ stmt.id ];
        send(Commands.COM_STMT_CLOSE, data);
    }

    ulong insertID() {
        return cast(size_t)status_.insertID;
    }

    ulong affected() {
        return cast(size_t)status_.affected;
    }

    size_t warnings() {
        return status_.warnings;
    }

    size_t error() {
        return status_.error;
    }

    const(char)[] status() {
        return info_;
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
        handshake(retrieve());
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

    void check(InputPacket packet) {
        auto id = packet.peek!ubyte;
        switch (id) {
            case StatusPackets.ERR_Packet:
            case StatusPackets.OK_Packet:
                status(packet);
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

    void handshake(InputPacket packet) {
        scope(failure) disconnect();

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

        status_.caps = cast(CapabilityFlags)(settings_.caps & server_.caps);

        auto reply = OutputPacket(&out_);
        reply.reserve(64 + settings_.user.length + settings_.pwd.length + settings_.db.length);
        
        reply.put!uint(status_.caps);
        reply.put!uint(1);
        reply.put!ubyte(33);
        reply.fill(0, 23);

        reply.put(settings_.user);
        reply.put!ubyte(0);

        if (settings_.pwd.length) {
            if (status_.caps & CapabilityFlags.CLIENT_SECURE_CONNECTION) {
                reply.put!ubyte(token.length);
                reply.put(token);
            } else {
                reply.put(token);
                reply.put!ubyte(0);
            }
        } else {
            reply.put!ubyte(0);
        }

        if (settings_.db.length && (status_.caps & CapabilityFlags.CLIENT_CONNECT_WITH_DB)) {
            reply.put(settings_.db);
            reply.put!ubyte(0);
        }

        reply.finalize(seq_);
        ++seq_;

        socket_.write(reply.get());

        status(retrieve());
    }

    void status(InputPacket packet) {
        auto id = packet.eat!ubyte;

        switch (id) {
        case StatusPackets.OK_Packet:
            status_.error = 0;
            status_.affected = packet.eatLenEnc();
            status_.insertID = packet.eatLenEnc();
            status_.flags = packet.eat!ushort;
            status_.warnings = packet.eat!ushort;

            if (status_.caps & CapabilityFlags.CLIENT_SESSION_TRACK) {
                info(packet.eat!(const(char)[])(cast(size_t)packet.eatLenEnc()));
                packet.skip(1);

                if (status_.flags & StatusFlags.SERVER_SESSION_STATE_CHANGED) {
                    packet.skip(cast(size_t)packet.eatLenEnc());
                    packet.skip(1);
                }
            } else if (!packet.empty) {
                info(packet.eat!(const(char)[])(cast(size_t)packet.eatLenEnc()));
            }
            break;
        case StatusPackets.EOF_Packet:
            status_.warnings = packet.eat!ushort;
            status_.flags = packet.eat!ushort;
            info([]);
            break;
        case StatusPackets.ERR_Packet:
            status_.flags = 0;
            status_.error = packet.eat!ushort;
            packet.skip(6);
            info(packet.eat!(const(char)[])(packet.remaining));

            throw new MySQLErrorException(cast(string)info_);
        default:
            throw new MySQLProtocolException("Unexpected packet format");
        }
    }

    void info(const(char)[] value) {
        info_.length = value.length;
        info_[0..$] = value;
    }

    void columnDef(InputPacket packet, Commands cmd, ref MySQLColumn def) {
        auto catalog = packet.eat!(const(char)[])(cast(size_t)packet.eatLenEnc());
        auto schema = packet.eat!(const(char)[])(cast(size_t)packet.eatLenEnc());
        auto table = packet.eat!(const(char)[])(cast(size_t)packet.eatLenEnc());
        auto org_table = packet.eat!(const(char)[])(cast(size_t)packet.eatLenEnc());
        def.name = packet.eat!(const(char)[])(cast(size_t)packet.eatLenEnc()).idup; // todo: fix allocation
        auto org_name = packet.eat!(const(char)[])(cast(size_t)packet.eatLenEnc());
        auto next_length = cast(size_t)packet.eatLenEnc();
        auto char_set = packet.eat!ushort;
        def.length = packet.eat!uint;
        def.type = cast(ColumnTypes)packet.eat!ubyte;
        def.flags = packet.eat!ushort;
        def.decimals = packet.eat!ubyte;

        packet.expect!ushort(0);

        if (cmd == Commands.COM_FIELD_LIST) {
            auto default_values = packet.eat!(const(char)[])(cast(size_t)packet.eatLenEnc());
        }
    }

    auto columnDefs(size_t count, Commands cmd) {
        header_.length = count;
        foreach (i; 0..count)
            columnDef(retrieve(), cmd, header_[i]);
        return header_;
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
        auto columns = cast(size_t)packet.eatLenEnc();
        auto header = columnDefs(columns, cmd);
        row_.length = columns;
        row_.header(header);

        size_t index = 0;
        auto statusFlags = skipEOF(retrieve());
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
                        statusFlags = skipEOF(row);
                        break;
                    }

                    resultSetRow(row, Commands.COM_STMT_FETCH, header, row_);
                    if (!callHandler(handler, index++, header, row_)) {
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
                    status(row);
                    break;
                }

                resultSetRow(row, cmd, header, row_);
                if (!callHandler(handler, index++, header, row_)) {
                    discardUntilEOF(retrieve());
                    break;
                }

                row = retrieve();
            }
        }
    }

    void discardAll(InputPacket packet, Commands cmd) {
        auto columns = cast(size_t)packet.eatLenEnc();
        auto defs = columnDefs(columns, cmd);

        auto statusFlags = skipEOF(retrieve());
        if ((statusFlags & StatusFlags.SERVER_STATUS_CURSOR_EXISTS) == 0) {
            while (true) {
                auto row = retrieve();
                if (row.peek!ubyte == StatusPackets.EOF_Packet) {
                    status(row);
                    break;
                }
            }
        }
    }

    void discardUntilEOF(InputPacket packet) {
        if (packet.peek!ubyte == StatusPackets.EOF_Packet) {
            status(packet);
            return;
        } else {
            while (true) {
                if (packet.peek!ubyte == StatusPackets.EOF_Packet) {
                    status(packet);
                    break;
                }
                packet = retrieve();
            }
        }
    }

    auto skipEOF(InputPacket packet) {
        auto id = packet.eat!ubyte;
        if (id != StatusPackets.EOF_Packet)
            throw new MySQLProtocolException("Unexpected packet format");
        
        packet.skip(2);
        return packet.eat!ushort();
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
    char[] info_;
    ubyte[] in_;
    ubyte[] out_;
    ubyte seq_ = 0;

    ConnectionStatus status_;
    ConnectionSettings settings_;
    ServerInfo server_;
}