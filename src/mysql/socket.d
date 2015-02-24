module mysql.socket;

import vibe.core.net;


struct VibeSocket {
    void connect(const(char)[] host, ushort port) {
        socket_ = connectTCP(cast(string)host, port);
    }

    bool connected() const {
        return socket_.connected();
    }

    void close() {
        socket_.close();
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