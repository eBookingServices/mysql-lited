module mysql.inserter;


import std.array;
import std.meta;
import std.range;
import std.traits;


import mysql.appender;


enum OnDuplicate : size_t {
	Ignore,
	Error,
	Replace,
	Update,
	UpdateAll,
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

	void start(Args...)(string tableName, Args fieldNames) if ((Args.length > 0) && (allSatisfy!(isSomeString, Args) || ((Args.length == 1) && isSomeString!(ElementType!(Args[0]))))) {
		start(OnDuplicate.Error, tableName, fieldNames);
	}

	void start(Args...)(OnDuplicate action, string tableName, Args fieldNames) if ((Args.length > 0) && (allSatisfy!(isSomeString, Args) || ((Args.length == 1) && isSomeString!(ElementType!(Args[0]))))) {
		fields_ = fieldNames.length;

		Appender!(char[]) app;

		final switch(action) with (OnDuplicate) {
		case Ignore:
			app.put("insert ignore into ");
			break;
		case Replace:
			app.put("replace into ");
			break;
		case UpdateAll:
			Appender!(char[]) dupapp;

			static if (isSomeString!(Args[0])) {
				alias Columns = fieldNames;
			} else {
				auto Columns = fieldNames[0];
			}

			foreach(size_t i, name; Columns) {
				dupapp.put('`');
				dupapp.put(name);
				dupapp.put("`=values(`");
				dupapp.put(name);
				dupapp.put("`)");
				if (i + 1 != Columns.length)
					dupapp.put(',');
			}
			dupUpdate_ = dupapp.data;
			goto case Update;
		case Update:
		case Error:
			app.put("insert into ");
			break;
		}

		app.put(tableName);
		app.put('(');

		static if (isSomeString!(Args[0])) {
			alias Columns = fieldNames;
		} else {
			auto Columns = fieldNames[0];
		}

		foreach(size_t i, name; Columns) {
			app.put('`');
			app.put(name);
			app.put('`');
			if (i + 1 != Columns.length)
				app.put(',');
		}

		app.put(")values");
		start_ = app.data;
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

			auto sql = cast(char[])values_.data();
			values_.clear;
			pending_ = 0;

			conn_.execute(sql);
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
