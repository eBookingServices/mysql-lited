module mysql.inserter;


import std.array;
import std.meta;
import std.range;
import std.string;
import std.traits;


import mysql.appender;
import mysql.exception;


enum OnDuplicate : size_t {
	Ignore,
	Error,
	Replace,
	Update,
	UpdateAll,
}


auto inserter(ConnectionType)(auto ref ConnectionType connection) {
	return Inserter!ConnectionType(connection);
}


auto inserter(ConnectionType, Args...)(auto ref ConnectionType connection, OnDuplicate action, string tableName, Args columns) {
	auto insert = Inserter!ConnectionType(&connection);
	insert.start(action, tableName, columns);
	return insert;
}


auto inserter(ConnectionType, Args...)(auto ref ConnectionType connection, string tableName, Args columns) {
	auto insert = Inserter!ConnectionType(&connection);
	insert.start(OnDuplicate.Error, tableName, columns);
	return insert;
}


private template isSomeStringOrSomeStringArray(T) {
	enum isSomeStringOrSomeStringArray = isSomeString!T || (isArray!T && isSomeString!(ElementType!T));
}


struct Inserter(ConnectionType) {
	@disable this();
	@disable this(this);

	this(ConnectionType* connection) {
		conn_ = connection;
		pending_ = 0;
		flushes_ = 0;
	}

	~this() {
		flush();
	}

	void start(Args...)(string tableName, Args fieldNames) if (Args.length && allSatisfy!(isSomeStringOrSomeStringArray, Args)) {
		start(OnDuplicate.Error, tableName, fieldNames);
	}

	void start(Args...)(OnDuplicate action, string tableName, Args fieldNames) if (Args.length && allSatisfy!(isSomeStringOrSomeStringArray, Args)) {
		auto fieldCount = fieldNames.length;

		foreach (size_t i, Arg; Args) {
			static if (isArray!Arg && !isSomeString!Arg) {
				fieldCount = (fieldCount - 1) + fieldNames[i].length;
			}
		}

		fields_ = fieldCount;

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

			foreach(size_t i, Arg; Args) {
				static if (isSomeString!Arg) {
					dupapp.put('`');
					dupapp.put(fieldNames[i]);
					dupapp.put("`=values(`");
					dupapp.put(fieldNames[i]);
					dupapp.put("`)");
				} else {
					auto columns = fieldNames[i];
					foreach (j, name; columns) {
						dupapp.put('`');
						dupapp.put(name);
						dupapp.put("`=values(`");
						dupapp.put(name);
						dupapp.put("`)");
						if (j + 1 != columns.length)
							dupapp.put(',');
					}
				}
				if (i + 1 != Args.length)
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

		foreach(size_t i, Arg; Args) {
			static if (isSomeString!Arg) {
				app.put('`');
				app.put(fieldNames[i]);
				app.put('`');
			} else {
				auto columns = fieldNames[i];
				foreach (j, name; columns) {
					app.put('`');
					app.put(name);
					app.put('`');
					if (j + 1 != columns.length)
						app.put(',');
				}
			}
			if (i + 1 != Args.length)
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
		if (start_.empty)
			throw new MySQLErrorException("Inserter must be initialized with a call to start()");

		auto valueCount = values.length;

		foreach (size_t i, Value; Values) {
			static if (isArray!Value && !isSomeString!Value) {
				valueCount = (valueCount - 1) + values[i].length;
			}
		}

		if (valueCount != fields_)
			throw new MySQLErrorException(format("Wrong number of parameters for row. Got %d but expected %d.", valueCount, fields_));

		if (!pending_)
			values_.put(cast(char[])start_);

		values_.put(pending_ ? ",(" : "(");
		++pending_;
		foreach (size_t i, Value; Values) {
			static if (isArray!Value && !isSomeString!Value) {
				appendValues(values_, values[i]);
			} else {
				appendValue(values_, values[i]);
			}
			if (i != values.length-1)
				values_.put(',');
		}
		values_.put(')');

		if (values_.data.length > (128 << 10)) // todo: make parameter
			flush();

		++rows_;
	}

	@property size_t rows() const {
		return rows_ != 0;
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

	ConnectionType* conn_;
	size_t pending_;
	size_t flushes_;
	size_t fields_;
	size_t rows_;
}
