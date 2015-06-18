module mysql.inserter;


import std.array;
import std.traits;


import mysql.appender;


enum OnDuplicate : size_t {
	Ignore,
	Error,
	Replace,
	Update,
}


auto inserter(ConnectionType)(ConnectionType connection) {
	return Inserter!ConnectionType(connection);
}


auto inserter(ConnectionType, Args...)(ConnectionType connection, OnDuplicate action, string tableName, Args columns) {
	auto insert = Inserter!ConnectionType(connection);
	insert.start(action, tableName, columns);
	return insert;
}


auto inserter(ConnectionType, Args...)(ConnectionType connection, string tableName, Args columns) {
	auto insert = Inserter!ConnectionType(connection);
	insert.start(OnDuplicate.Error, tableName, columns);
	return insert;
}


struct Inserter(ConnectionType) {
	@disable this();

	this(ConnectionType connection) {
		conn_ = connection;
		pending_ = 0;
		flushes_ = 0;
	}

	~this() {
		flush();
	}

	void start(Args...)(string tableName, Args fieldNames) {
		start(OnDuplicate.Error, tableName, fieldNames);
	}

	void start(Args...)(OnDuplicate action, string tableName, Args fieldNames) {
		fields_ = fieldNames.length;

		Appender!(char[]) appender;

		final switch(action) {
			case OnDuplicate.Ignore:
				appender.put("insert ignore into ");
				break;
			case OnDuplicate.Replace:
				appender.put("replace into ");
				break;
			case OnDuplicate.Update:
			case OnDuplicate.Error:
				appender.put("insert into ");
				break;
		}

		appender.put(tableName);
		appender.put('(');
		foreach(size_t i, name; fieldNames) {
			appender.put('`');
			appender.put(name);
			appender.put('`');
			if (i != fieldNames.length-1)
				appender.put(',');
		}
		appender.put(")values");
		start_ = appender.data;
	}

	auto ref duplicateUpdate(string update) {
		dupUpdate_ = cast(char[])update;
		return this;
	}

	void row(Values...)(Values values) {
		assert(values.length == fields_, "Column count and value count must match");
		assert(!start_.empty, "Must call start before inserting a row");

		if (!pending_)
			values_.put(cast(char[])start_);

		values_.put(pending_ ? ",(" : "(");
		++pending_;
		foreach (size_t i, value; values) {
			appendValue(values_, value);
			if (i != values.length-1)
				values_.put(',');
		}
		values_.put(')');

		if (values_.data.length > (128 << 10)) // todo: make parameter
			flush();
	}

	@property size_t pending() const {
		return pending_ != 0;
	}

	@property size_t flushes() const {
		return flushes_;
	}

	void flush() {
		if (pending_) {
			if (dupUpdate_.length) {
				values_.put(cast(ubyte[])" on duplicate key update ");
				values_.put(cast(ubyte[])dupUpdate_);
			}

			conn_.execute(cast(char[])values_.data());
			values_.clear;
			pending_ = 0;
			++flushes_;
		}
	}

private:
	char[] start_;
	char[] dupUpdate_;
	Appender!(char[]) values_;

	ConnectionType conn_;
	size_t pending_;
	size_t flushes_;
	size_t fields_;
}
