module mysql.socket;

import vibe.core.net;


struct VibeSocket {
	~this() {
		close();
	}

	void connect(const(char)[] host, ushort port) {
		socket_ = connectTCP(cast(string)host, port);
		socket_.keepAlive = true;
		socket_.tcpNoDelay = true;
	}

	bool connected() const {
		return socket_.connected();
	}

	void close() {
		if (socket_) {
			socket_.close();
			socket_ = null;
		}
	}

	void read(ubyte[] buffer) {
		socket_.read(buffer);
	}

	void write(in ubyte[] buffer) {
		socket_.write(buffer);
	}

	void flush() {
		socket_.flush();
	}

	bool empty() {
		return socket_.empty;
	}

private:
	TCPConnection socket_;
}
