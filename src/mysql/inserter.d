module mysql.inserter;


import std.array;
import std.algorithm;
import std.conv;
import std.datetime;
import std.range;
import std.string;
import std.traits;


import mysql.protocol;
import mysql.type;


struct RawString {
	@disable this();

	this(string x) {
		value_ = x;
	}

	package string value_;
}


private void appendValue(T)(ref Appender!(char[]) appender, T value) if (is(typeof(T) == typeof(null))) {
	appender.put("null");
}

private void appendValue(T)(ref Appender!(char[]) appender, T value) if (isScalarType!T) {
	appender.put(cast(ubyte[])to!string(value));
}

private void appendValue(T)(ref Appender!(char[]) appender, T value) if (is(Unqual!T == SysTime)) {
	appendValue(appender, RawString(value.toUTC().toISOString()));
}

private void appendValue(T)(ref Appender!(char[]) appender, T value) if (is(Unqual!T == DateTime)) {
	appendValue(appender, RawString(value.toISOString()));
}

private void appendValue(T)(ref Appender!(char[]) appender, T value) if (is(Unqual!T == Date)) {
	appendValue(appender, RawString(value.toISOString()));
}

private void appendValue(T)(ref Appender!(char[]) appender, T value) if (is(Unqual!T == RawString)) {
	appender.put('\'');
	appender.put(cast(char[])value.value_);
	appender.put('\'');
}

private void appendValue(T)(ref Appender!(char[]) appender, T value) if (is(Unqual!T == MySQLValue)) {
	final switch(value.type) with (ColumnTypes) {
	case MYSQL_TYPE_NULL:
		appender.put("null");
		break;
	case MYSQL_TYPE_TINY:
		if (value.isSigned) {
			appendValue(appender, value.peek!byte);
		} else {
			appendValue(appender, value.peek!ubyte);
		}
		break;
	case MYSQL_TYPE_YEAR:
	case MYSQL_TYPE_SHORT:
		if (value.isSigned) {
			appendValue(appender, value.peek!short);
		} else {
			appendValue(appender, value.peek!ushort);
		}
		break;
	case MYSQL_TYPE_INT24:
	case MYSQL_TYPE_LONG:
		if (value.isSigned) {
			appendValue(appender, value.peek!int);
		} else {
			appendValue(appender, value.peek!uint);
		}
		break;
	case MYSQL_TYPE_LONGLONG:
		if (value.isSigned) {
			appendValue(appender, value.peek!long);
		} else {
			appendValue(appender, value.peek!ulong);
		}
		break;
	case MYSQL_TYPE_DOUBLE:
		appendValue(appender, value.peek!double);
		break;
	case MYSQL_TYPE_FLOAT:
		appendValue(appender, value.peek!float);
		break;
	case MYSQL_TYPE_SET:
	case MYSQL_TYPE_ENUM:
	case MYSQL_TYPE_VARCHAR:
	case MYSQL_TYPE_VAR_STRING:
	case MYSQL_TYPE_STRING:
	case MYSQL_TYPE_NEWDECIMAL:
	case MYSQL_TYPE_DECIMAL:
		appendValue(appender, value.peek!(char[]));
		break;
	case MYSQL_TYPE_BIT:
	case MYSQL_TYPE_TINY_BLOB:
	case MYSQL_TYPE_MEDIUM_BLOB:
	case MYSQL_TYPE_LONG_BLOB:
	case MYSQL_TYPE_BLOB:
	case MYSQL_TYPE_GEOMETRY:
		appendValue(appender, value.peek!(ubyte[]));
		break;
	case MYSQL_TYPE_TIME:
	case MYSQL_TYPE_TIME2:
		appendValue(appender, value.peek!DateTime);
		break;
	case MYSQL_TYPE_DATE:
	case MYSQL_TYPE_NEWDATE:
	case MYSQL_TYPE_DATETIME:
	case MYSQL_TYPE_DATETIME2:
	case MYSQL_TYPE_TIMESTAMP:
	case MYSQL_TYPE_TIMESTAMP2:
		appendValue(appender, value.peek!DateTime);
		break;
	}
}

private void appendValue(T)(ref Appender!(char[]) appender, T value) if (isArray!T && (is(Unqual!(typeof(T.init[0])) == ubyte) || is(Unqual!(typeof(T.init[0])) == char))) {
	appender.put('\'');
	auto ptr = value.ptr;
	auto end = value.ptr + value.length;
	while (ptr != end) {
		switch(*ptr) {
		case '\\':
		case '\'':
			appender.put('\\');
			goto default;
		default:
			appender.put(*ptr++);
		}
	}
	appender.put('\'');
}


enum OnDuplicate : size_t {
	Ignore,
	Error,
	Replace,
	Update,
}


auto inserter(ConnectionType)(ConnectionType connection) {
	return Inserter!ConnectionType(connection);
}


auto inserter(ConnectionType, Args...)(ConnectionType connection, string tableName, OnDuplicate action, Args columns) {
	auto insert = Inserter!ConnectionType(connection);
	insert.start(tableName, action, columns);
	return insert;
}


auto inserter(ConnectionType, Args...)(ConnectionType connection, string tableName, Args columns) {
	auto insert = Inserter!ConnectionType(connection);
	insert.start(tableName, OnDuplicate.Error, columns);
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

	void start(Args...)(string tableName, OnDuplicate action, Args fieldNames) {
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
