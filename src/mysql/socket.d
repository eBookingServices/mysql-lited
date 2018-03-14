module mysql.socket;

import vibe.core.net;
import vibe.stream.tls;

import mysql.ssl;

struct VibeSocket {
	void connect(const(char)[] host, ushort port) {
		socket_ = connectTCP(cast(string)host, port);
		socket_.keepAlive = true;
		socket_.tcpNoDelay = true;
		stream_ = socket_;
	}

	bool connected() inout {
		return socket_ && socket_.connected();
	}

	void close() {
		if (socket_) {
			socket_.close();
			socket_ = null;
		}
	}

	void read(ubyte[] buffer) {
		stream_.read(buffer);
	}

	void write(in ubyte[] buffer) {
		stream_.write(buffer);
	}

	void flush() {
		stream_.flush();
	}

	bool empty() {
		return stream_.empty;
	}

	void startSSL(const(char)[] hostName, SSLConfig config) {
		TLSVersion tlsVersion;

		final switch (config.sslVersion) with (SSLConfig.Version) {
		case any:
			tlsVersion = TLSVersion.any;
			break;
		case ssl3:
			tlsVersion = TLSVersion.ssl3;
			break;
		case tls1:
			tlsVersion = TLSVersion.tls1;
			break;
		case tls1_1:
			tlsVersion = TLSVersion.tls1_1;
			break;
		case tls1_2:
			tlsVersion = TLSVersion.tls1_2;
			break;
		case dtls1:
			tlsVersion = TLSVersion.dtls1;
			break;
		}

		TLSPeerValidationMode peerValidationMode;

		final switch (config.validate) with (SSLConfig.Validate) {
		case basic:
			peerValidationMode = TLSPeerValidationMode.checkCert | TLSPeerValidationMode.requireCert;
			break;
		case trust:
			peerValidationMode = TLSPeerValidationMode.checkCert | TLSPeerValidationMode.requireCert | TLSPeerValidationMode.checkTrust;
			break;
		case identity:
			peerValidationMode = TLSPeerValidationMode.checkCert | TLSPeerValidationMode.requireCert | TLSPeerValidationMode.checkTrust | TLSPeerValidationMode.checkPeer;
			break;
		}

		auto ctx = createTLSContext(TLSContextKind.client, tlsVersion);
		ctx.peerValidationMode = peerValidationMode;

		if (config.rootCertFile.length)
			ctx.useTrustedCertificateFile(config.rootCertFile.idup);

		if (config.ciphers.length)
			ctx.setCipherList(config.ciphers.idup);

		auto peerName = config.hostName.length ? config.hostName : hostName;

		stream_ = createTLSStream(socket_, ctx, TLSStreamState.connecting, peerName.idup, socket_.remoteAddress);
	}

private:
	TCPConnection socket_;
	Stream stream_;
}
