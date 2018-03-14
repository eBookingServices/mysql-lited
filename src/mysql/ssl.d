module mysql.ssl;


static struct SSLConfig {
	enum Version {
		any = 0,
		ssl3,
		tls1,
		tls1_1,
		tls1_2,
		dtls1,
	}

	enum Validate {
		basic = 0,
		trust,
		identity,
	}

	bool enforce;

	Version sslVersion = Version.any;
	Validate validate = Validate.basic;

	const(char)[] hostName;
	const(char)[] rootCertFile;
	const(char)[] ciphers;
}
