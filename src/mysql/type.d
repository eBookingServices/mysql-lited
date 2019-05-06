module mysql.type;


import std.algorithm;
import std.array : appender;
import std.conv : parse, to;
import std.datetime;
import std.format: format, formattedWrite;
import std.traits;
import std.typecons;

import mysql.protocol;
import mysql.packet;
import mysql.exception;
public import mysql.row;

struct IgnoreAttribute {}
struct OptionalAttribute {}
struct NameAttribute { const(char)[] name; }
struct UnCamelCaseAttribute {}
struct TableNameAttribute {const(char)[] name;}

@property TableNameAttribute tableName(const(char)[] name) {
	return TableNameAttribute(name);
}

@property IgnoreAttribute ignore() {
	return IgnoreAttribute();
}


@property OptionalAttribute optional() {
	return OptionalAttribute();
}


@property NameAttribute as(const(char)[] name)  {
	return NameAttribute(name);
}


@property UnCamelCaseAttribute uncamel() {
	return UnCamelCaseAttribute();
}


template Unnull(U) {
	alias impl(N : Nullable!T, T) = T;
	alias impl(T) = T;
	alias Unnull = impl!U;
}

alias Unboth(T) = Unqual!(Unnull!T);
enum isSomeDuration(T) = is(Unboth!T == Date) || is(Unboth!T == DateTime) || is(Unboth!T == SysTime) || is(Unboth!T == Duration);
enum isValueType(T) = isSomeDuration!(Unboth!T) || is(Unboth!T == MySQLValue) || (!is(Unboth!T == struct) && !is(Unboth!T == class));

template isWritableDataMember(T, string Member) {
	static if (is(TypeTuple!(__traits(getMember, T, Member)))) {
		enum isWritableDataMember = false;
	} else static if (!is(typeof(__traits(getMember, T, Member)))) {
		enum isWritableDataMember = false;
	} else static if (is(typeof(__traits(getMember, T, Member)) == void)) {
		enum isWritableDataMember = false;
	} else static if (is(typeof(__traits(getMember, T, Member)) == enum)) {
		enum isWritableDataMember = true;
	} else static if (hasUDA!(__traits(getMember, T, Member), IgnoreAttribute)) {
		enum isWritableDataMember = false;
	} else static if (isArray!(typeof(__traits(getMember, T, Member))) && !is(typeof(typeof(__traits(getMember, T, Member)).init[0]) == ubyte) && !is(typeof(__traits(getMember, T, Member)) == string)) {
		enum isWritableDataMember = false;
	} else static if (isAssociativeArray!(typeof(__traits(getMember, T, Member)))) {
		enum isWritableDataMember = false;
	} else static if (isSomeFunction!(typeof(__traits(getMember, T, Member)))) {
		enum isWritableDataMember = false;
	} else static if (!is(typeof((){ T x = void; __traits(getMember, x, Member) = __traits(getMember, x, Member); }()))) {
		enum isWritableDataMember = false;
	} else static if ((__traits(getProtection, __traits(getMember, T, Member)) != "public") && (__traits(getProtection, __traits(getMember, T, Member)) != "export")) {
		enum isWritableDataMember = false;
	} else {
		enum isWritableDataMember = true;
	}
}

template isReadableDataMember(T, string Member) {
	static if (is(TypeTuple!(__traits(getMember, T, Member)))) {
		enum isReadableDataMember = false;
	} else static if (!is(typeof(__traits(getMember, T, Member)))) {
		enum isReadableDataMember = false;
	} else static if (is(typeof(__traits(getMember, T, Member)) == void)) {
		enum isReadableDataMember = false;
	} else static if (is(typeof(__traits(getMember, T, Member)) == enum)) {
		enum isReadableDataMember = true;
	} else static if (hasUDA!(__traits(getMember, T, Member), IgnoreAttribute)) {
		enum isReadableDataMember = false;
	} else static if (isArray!(typeof(__traits(getMember, T, Member))) && !is(typeof(typeof(__traits(getMember, T, Member)).init[0]) == ubyte) && !is(typeof(__traits(getMember, T, Member)) == string)) {
		enum isReadableDataMember = false;
	} else static if (isAssociativeArray!(typeof(__traits(getMember, T, Member)))) {
		enum isReadableDataMember = false;
	} else static if (isSomeFunction!(typeof(__traits(getMember, T, Member)))  /* && return type is valueType*/ ) {
		enum isReadableDataMember = true;
	} else static if (!is(typeof((){ T x = void; __traits(getMember, x, Member) = __traits(getMember, x, Member); }()))) {
		enum isReadableDataMember = false;
	} else static if ((__traits(getProtection, __traits(getMember, T, Member)) != "public") && (__traits(getProtection, __traits(getMember, T, Member)) != "export")) {
		enum isReadableDataMember = false;
	} else {
		enum isReadableDataMember = true;
	}
}

struct MySQLRawString {
	@disable this();

	this(const(char)[] data) {
		data_ = data;
	}

	@property auto length() const {
		return data_.length;
	}

	@property auto data() const {
		return data_;
	}

	private const(char)[] data_;
}



struct MySQLFragment {
	@disable this();

	this(const(char)[] data) {
		data_ = data;
	}

	@property auto length() const {
		return data_.length;
	}

	@property auto data() const {
		return data_;
	}

	private const(char)[] data_;
}


struct MySQLBinary {
	this(T)(T[] data) {
		data_ = (cast(ubyte*)data.ptr)[0..typeof(T[].init[0]).sizeof * data.length];
	}

	@property auto length() const {
		return data_.length;
	}

	@property auto data() const {
		return data_;
	}

	private const(ubyte)[] data_;
}


struct MySQLValue {
	package enum BufferSize = max(ulong.sizeof, (ulong[]).sizeof, MySQLDateTime.sizeof, MySQLTime.sizeof);
	package this(const(char)[] name, ColumnTypes type, bool signed, void* ptr, size_t size) {
		assert(size <= BufferSize);
		type_ = type;
		sign_ = signed ? 0x00 : 0x80;
		if (type != ColumnTypes.MYSQL_TYPE_NULL)
			buffer_[0..size] = (cast(ubyte*)ptr)[0..size];
		name_ = name;
	}

	this(T)(T) if (is(Unqual!T == typeof(null))) {
		type_ = ColumnTypes.MYSQL_TYPE_NULL;
		sign_ = 0x00;
	}

	this(T)(T value) if (is(Unqual!T == MySQLValue)) {
		this = value;
	}

	this(T)(T value) if (std.traits.isFloatingPoint!T) {
		alias UT = Unqual!T;

		sign_ = 0x00;
		static if (is(UT == float)) {
			type_ = ColumnTypes.MYSQL_TYPE_FLOAT;
			buffer_[0..T.sizeof] = (cast(ubyte*)&value)[0..T.sizeof];
		} else static if (is(UT == double)) {
			type_ = ColumnTypes.MYSQL_TYPE_DOUBLE;
			buffer_[0..T.sizeof] = (cast(ubyte*)&value)[0..T.sizeof];
		} else {
			type_ = ColumnTypes.MYSQL_TYPE_DOUBLE;
			auto data = cast(double)value;
			buffer_[0..typeof(data).sizeof] = (cast(ubyte*)&data)[0..typeof(data).sizeof];
		}
	}

	this(T)(T value) if (isIntegral!T || isBoolean!T) {
		alias UT = Unqual!T;

		static if (is(UT == long) || is(UT == ulong)) {
			type_ = ColumnTypes.MYSQL_TYPE_LONGLONG;
		} else static if (is(UT == int) || is(UT == uint) || is(UT == dchar)) {
			type_ = ColumnTypes.MYSQL_TYPE_LONG;
		} else static if (is(UT == short) || is(UT == ushort) || is(UT == wchar)) {
			type_ = ColumnTypes.MYSQL_TYPE_SHORT;
		} else {
			type_ = ColumnTypes.MYSQL_TYPE_TINY;
		}

		sign_ = isUnsigned!UT ? 0x80 : 0x00;
		buffer_[0..T.sizeof] = (cast(ubyte*)&value)[0..T.sizeof];
	}

	this(T)(T value) if (is(Unqual!T == Date) || is(Unqual!T == DateTime) || is(Unqual!T == SysTime)) {
		type_ = ColumnTypes.MYSQL_TYPE_TIMESTAMP;
		sign_ = 0x00;
		(*cast(MySQLDateTime*)buffer_) = MySQLDateTime.from(value);
	}

	this(T)(T value) if (is(Unqual!T == Duration) || is(Unqual!T == TimeOfDay)) {
		type_ = ColumnTypes.MYSQL_TYPE_TIME;
		sign_ = 0x00;
		(*cast(MySQLTime*)buffer_) = MySQLTime.from(value);
	}

	this(T)(T value) if (isSomeString!(OriginalType!T)) {
		static assert(typeof(T.init[0]).sizeof == 1, format("Unsupported string type: %s", T.stringof));

		type_ = ColumnTypes.MYSQL_TYPE_STRING;
		sign_ = 0x80;

		auto slice = value[0..$];
		buffer_.ptr[0..typeof(slice).sizeof] = (cast(ubyte*)&slice)[0..typeof(slice).sizeof];
	}

	this(T)(T value) if (is(Unqual!T == MySQLBinary)) {
		type_ = ColumnTypes.MYSQL_TYPE_BLOB;
		sign_ = 0x80;
		buffer_.ptr[0..(ubyte[]).sizeof] = (cast(ubyte*)&value.data_)[0..(ubyte[]).sizeof];
	}

	void toString(Appender)(ref Appender app) const {
		final switch(type_) with (ColumnTypes) {
		case MYSQL_TYPE_NULL:
			break;
		case MYSQL_TYPE_TINY:
			if (isSigned) formattedWrite(&app, "%d", *cast(ubyte*)buffer_.ptr);
			else formattedWrite(&app, "%d", *cast(byte*)buffer_.ptr);
			break;
		case MYSQL_TYPE_YEAR:
		case MYSQL_TYPE_SHORT:
			if (isSigned) formattedWrite(&app, "%d", *cast(short*)buffer_.ptr);
			else formattedWrite(&app, "%d", *cast(ushort*)buffer_.ptr);
			break;
		case MYSQL_TYPE_INT24:
		case MYSQL_TYPE_LONG:
			if (isSigned) formattedWrite(&app, "%d", *cast(int*)buffer_.ptr);
			else formattedWrite(&app, "%d", *cast(uint*)buffer_.ptr);
			break;
		case MYSQL_TYPE_LONGLONG:
			if (isSigned) formattedWrite(&app, "%d", *cast(long*)buffer_.ptr);
			else formattedWrite(&app, "%d", *cast(ulong*)buffer_.ptr);
			break;
		case MYSQL_TYPE_FLOAT:
			formattedWrite(&app, "%g", *cast(float*)buffer_.ptr);
			break;
		case MYSQL_TYPE_DOUBLE:
			formattedWrite(&app, "%g", *cast(double*)buffer_.ptr);
			break;
		case MYSQL_TYPE_SET:
		case MYSQL_TYPE_ENUM:
		case MYSQL_TYPE_VARCHAR:
		case MYSQL_TYPE_VAR_STRING:
		case MYSQL_TYPE_STRING:
		case MYSQL_TYPE_JSON:
		case MYSQL_TYPE_NEWDECIMAL:
		case MYSQL_TYPE_DECIMAL:
		case MYSQL_TYPE_TINY_BLOB:
		case MYSQL_TYPE_MEDIUM_BLOB:
		case MYSQL_TYPE_LONG_BLOB:
		case MYSQL_TYPE_BLOB:
			app.put(*cast(string*)buffer_.ptr);
			break;
		case MYSQL_TYPE_BIT:
		case MYSQL_TYPE_GEOMETRY:
			formattedWrite(&app, "%s", *cast(ubyte[]*)buffer_.ptr);
			break;
		case MYSQL_TYPE_TIME:
		case MYSQL_TYPE_TIME2:
			formattedWrite(&app, "%s", (*cast(MySQLTime*)buffer_.ptr).to!Duration());
			break;
		case MYSQL_TYPE_DATE:
		case MYSQL_TYPE_NEWDATE:
		case MYSQL_TYPE_DATETIME:
		case MYSQL_TYPE_DATETIME2:
		case MYSQL_TYPE_TIMESTAMP:
		case MYSQL_TYPE_TIMESTAMP2:
			formattedWrite(&app, "%s", (*cast(MySQLDateTime*)buffer_.ptr).to!DateTime());
			break;
		}
	}

	string toString() const {
		auto app = appender!string;
		toString(app);
		return app.data;
	}

	bool opEquals(MySQLValue other) const {
		if (isString && other.isString) {
			return peek!string == other.peek!string;
		} else if (isScalar && other.isScalar) {
			if (isFloatingPoint || other.isFloatingPoint)
				return get!double == other.get!double;
			if (isSigned || other.isSigned)
				return get!long == other.get!long;
			return get!ulong == other.get!ulong;
		} else if (isTime && other.isTime) {
			return get!Duration == other.get!Duration;
		} else if (isTimestamp && other.isTimestamp) {
			return get!SysTime == other.get!SysTime;
		} else if (isNull && other.isNull) {
			return true;
		}
		return false;
	}

	T get(T)(lazy T def) const {
		return !isNull ? get!T : def;
	}

	T get(T)() const if (isScalarType!T && !is(T == enum)) {
		switch(type_) with (ColumnTypes) {
		case MYSQL_TYPE_TINY:
			return cast(T)(*cast(ubyte*)buffer_.ptr);
		case MYSQL_TYPE_YEAR:
		case MYSQL_TYPE_SHORT:
			return cast(T)(*cast(ushort*)buffer_.ptr);
		case MYSQL_TYPE_INT24:
		case MYSQL_TYPE_LONG:
			return cast(T)(*cast(uint*)buffer_.ptr);
		case MYSQL_TYPE_LONGLONG:
			return cast(T)(*cast(ulong*)buffer_.ptr);
		case MYSQL_TYPE_FLOAT:
			return cast(T)(*cast(float*)buffer_.ptr);
		case MYSQL_TYPE_DOUBLE:
			return cast(T)(*cast(double*)buffer_.ptr);
		default:
			throw new MySQLErrorException(format("Cannot convert '%s' from %s to %s", name_, columnTypeName(type_), T.stringof));
		}
	}

	T get(T)() const if (is(Unqual!T == SysTime) || is(Unqual!T == DateTime) || is(Unqual!T == Date)) {
		switch(type_) with (ColumnTypes) {
		case MYSQL_TYPE_DATE:
		case MYSQL_TYPE_NEWDATE:
		case MYSQL_TYPE_DATETIME:
		case MYSQL_TYPE_DATETIME2:
		case MYSQL_TYPE_TIMESTAMP:
		case MYSQL_TYPE_TIMESTAMP2:
			return (*cast(MySQLDateTime*)buffer_.ptr).to!T;
		default:
			throw new MySQLErrorException(format("Cannot convert '%s' from %s to %s", name_, columnTypeName(type_), T.stringof));
		}
	}

	T get(T)() const if (is(Unqual!T == TimeOfDay)) {
		switch(type_) with (ColumnTypes) {
		case MYSQL_TYPE_DATE:
		case MYSQL_TYPE_NEWDATE:
		case MYSQL_TYPE_DATETIME:
		case MYSQL_TYPE_DATETIME2:
		case MYSQL_TYPE_TIMESTAMP:
		case MYSQL_TYPE_TIMESTAMP2:
			return (*cast(MySQLDateTime*)buffer_.ptr).to!T;
		case MYSQL_TYPE_TIME:
		case MYSQL_TYPE_TIME2:
			return (*cast(MySQLTime*)buffer_.ptr).to!T;
		default:
			throw new MySQLErrorException(format("Cannot convert '%s' from %s to %s", name_, columnTypeName(type_), T.stringof));
		}
	}

	T get(T)() const if (is(Unqual!T == Duration)) {
		switch(type_) with (ColumnTypes) {
		case MYSQL_TYPE_TIME:
		case MYSQL_TYPE_TIME2:
			return (*cast(MySQLTime*)buffer_.ptr).to!T;
		default:
			throw new MySQLErrorException(format("Cannot convert '%s' from %s to %s", name_, columnTypeName(type_), T.stringof));
		}
	}

	T get(T)() const if (is(Unqual!T == enum)) {
		return cast(T)get!(OriginalType!T);
	}

	T get(T)() const if (isArray!T && !is(T == enum)) {
		switch(type_) with (ColumnTypes) {
		case MYSQL_TYPE_SET:
		case MYSQL_TYPE_ENUM:
		case MYSQL_TYPE_VARCHAR:
		case MYSQL_TYPE_VAR_STRING:
		case MYSQL_TYPE_STRING:
		case MYSQL_TYPE_JSON:
		case MYSQL_TYPE_NEWDECIMAL:
		case MYSQL_TYPE_DECIMAL:
			return (*cast(T*)buffer_.ptr).dup;
		case MYSQL_TYPE_BIT:
		case MYSQL_TYPE_TINY_BLOB:
		case MYSQL_TYPE_MEDIUM_BLOB:
		case MYSQL_TYPE_LONG_BLOB:
		case MYSQL_TYPE_BLOB:
		case MYSQL_TYPE_GEOMETRY:
			return (*cast(T*)buffer_.ptr).dup;
		default:
			throw new MySQLErrorException(format("Cannot convert '%s' from %s to %s", name_, columnTypeName(type_), T.stringof));
		}
	}

	T get(T)() const if(isInstanceOf!(Nullable, T)) {
		if (type_ == ColumnTypes.MYSQL_TYPE_NULL)
			return T.init;
		return T(get!(typeof(T.init.get)));
	}

	T peek(T)(lazy T def) const {
		return !isNull ? peek!(T) : def;
	}

	T peek(T)() const if (isScalarType!T) {
		return get!(T);
	}

	T peek(T)() const if (is(Unqual!T == SysTime) || is(Unqual!T == DateTime) || is(Unqual!T == Date) || is(Unqual!T == TimeOfDay)) {
		return get!(T);
	}

	T peek(T)() const if (is(Unqual!T == Duration)) {
		return get!(T);
	}

	T peek(T)() const if (isArray!T) {
		switch(type_) with (ColumnTypes) {
		case MYSQL_TYPE_SET:
		case MYSQL_TYPE_ENUM:
		case MYSQL_TYPE_VARCHAR:
		case MYSQL_TYPE_VAR_STRING:
		case MYSQL_TYPE_STRING:
		case MYSQL_TYPE_JSON:
		case MYSQL_TYPE_NEWDECIMAL:
		case MYSQL_TYPE_DECIMAL:
			return (*cast(T*)buffer_.ptr);
		case MYSQL_TYPE_BIT:
		case MYSQL_TYPE_TINY_BLOB:
		case MYSQL_TYPE_MEDIUM_BLOB:
		case MYSQL_TYPE_LONG_BLOB:
		case MYSQL_TYPE_BLOB:
		case MYSQL_TYPE_GEOMETRY:
			return (*cast(T*)buffer_.ptr);
		default:
			throw new MySQLErrorException(format("Cannot convert '%s' from %s to %s", name_, columnTypeName(type_), T.stringof));
		}
	}

	bool isNull() const {
		return type_ == ColumnTypes.MYSQL_TYPE_NULL;
	}

	ColumnTypes type() const {
		return type_;
	}

	bool isSigned() const {
		return sign_ == 0x00;
	}

	bool isString() const {
		final switch(type_) with (ColumnTypes) {
		case MYSQL_TYPE_NULL:
			return false;
		case MYSQL_TYPE_TINY:
		case MYSQL_TYPE_YEAR:
		case MYSQL_TYPE_SHORT:
		case MYSQL_TYPE_INT24:
		case MYSQL_TYPE_LONG:
		case MYSQL_TYPE_LONGLONG:
		case MYSQL_TYPE_FLOAT:
		case MYSQL_TYPE_DOUBLE:
			return false;
		case MYSQL_TYPE_SET:
		case MYSQL_TYPE_ENUM:
		case MYSQL_TYPE_VARCHAR:
		case MYSQL_TYPE_VAR_STRING:
		case MYSQL_TYPE_STRING:
		case MYSQL_TYPE_JSON:
		case MYSQL_TYPE_NEWDECIMAL:
		case MYSQL_TYPE_DECIMAL:
		case MYSQL_TYPE_TINY_BLOB:
		case MYSQL_TYPE_MEDIUM_BLOB:
		case MYSQL_TYPE_LONG_BLOB:
		case MYSQL_TYPE_BLOB:
			return true;
		case MYSQL_TYPE_BIT:
		case MYSQL_TYPE_GEOMETRY:
			return false;
		case MYSQL_TYPE_TIME:
		case MYSQL_TYPE_TIME2:
			return false;
		case MYSQL_TYPE_DATE:
		case MYSQL_TYPE_NEWDATE:
		case MYSQL_TYPE_DATETIME:
		case MYSQL_TYPE_DATETIME2:
		case MYSQL_TYPE_TIMESTAMP:
		case MYSQL_TYPE_TIMESTAMP2:
			return false;
		}
	}

	bool isScalar() const {
		final switch(type_) with (ColumnTypes) {
		case MYSQL_TYPE_NULL:
			return false;
		case MYSQL_TYPE_TINY:
		case MYSQL_TYPE_YEAR:
		case MYSQL_TYPE_SHORT:
		case MYSQL_TYPE_INT24:
		case MYSQL_TYPE_LONG:
		case MYSQL_TYPE_LONGLONG:
		case MYSQL_TYPE_FLOAT:
		case MYSQL_TYPE_DOUBLE:
			return true;
		case MYSQL_TYPE_SET:
		case MYSQL_TYPE_ENUM:
		case MYSQL_TYPE_VARCHAR:
		case MYSQL_TYPE_VAR_STRING:
		case MYSQL_TYPE_STRING:
		case MYSQL_TYPE_JSON:
		case MYSQL_TYPE_NEWDECIMAL:
		case MYSQL_TYPE_DECIMAL:
		case MYSQL_TYPE_TINY_BLOB:
		case MYSQL_TYPE_MEDIUM_BLOB:
		case MYSQL_TYPE_LONG_BLOB:
		case MYSQL_TYPE_BLOB:
			return false;
		case MYSQL_TYPE_BIT:
		case MYSQL_TYPE_GEOMETRY:
			return false;
		case MYSQL_TYPE_TIME:
		case MYSQL_TYPE_TIME2:
			return false;
		case MYSQL_TYPE_DATE:
		case MYSQL_TYPE_NEWDATE:
		case MYSQL_TYPE_DATETIME:
		case MYSQL_TYPE_DATETIME2:
		case MYSQL_TYPE_TIMESTAMP:
		case MYSQL_TYPE_TIMESTAMP2:
			return false;
		}
	}

	bool isFloatingPoint() const {
		final switch(type_) with (ColumnTypes) {
		case MYSQL_TYPE_NULL:
			return false;
		case MYSQL_TYPE_TINY:
		case MYSQL_TYPE_YEAR:
		case MYSQL_TYPE_SHORT:
		case MYSQL_TYPE_INT24:
		case MYSQL_TYPE_LONG:
		case MYSQL_TYPE_LONGLONG:
			return false;
		case MYSQL_TYPE_FLOAT:
		case MYSQL_TYPE_DOUBLE:
			return true;
		case MYSQL_TYPE_SET:
		case MYSQL_TYPE_ENUM:
		case MYSQL_TYPE_VARCHAR:
		case MYSQL_TYPE_VAR_STRING:
		case MYSQL_TYPE_STRING:
		case MYSQL_TYPE_JSON:
		case MYSQL_TYPE_NEWDECIMAL:
		case MYSQL_TYPE_DECIMAL:
		case MYSQL_TYPE_TINY_BLOB:
		case MYSQL_TYPE_MEDIUM_BLOB:
		case MYSQL_TYPE_LONG_BLOB:
		case MYSQL_TYPE_BLOB:
			return false;
		case MYSQL_TYPE_BIT:
		case MYSQL_TYPE_GEOMETRY:
			return false;
		case MYSQL_TYPE_TIME:
		case MYSQL_TYPE_TIME2:
			return false;
		case MYSQL_TYPE_DATE:
		case MYSQL_TYPE_NEWDATE:
		case MYSQL_TYPE_DATETIME:
		case MYSQL_TYPE_DATETIME2:
		case MYSQL_TYPE_TIMESTAMP:
		case MYSQL_TYPE_TIMESTAMP2:
			return false;
		}
	}

	bool isTime() const {
		final switch(type_) with (ColumnTypes) {
		case MYSQL_TYPE_NULL:
			return false;
		case MYSQL_TYPE_TINY:
		case MYSQL_TYPE_YEAR:
		case MYSQL_TYPE_SHORT:
		case MYSQL_TYPE_INT24:
		case MYSQL_TYPE_LONG:
		case MYSQL_TYPE_LONGLONG:
		case MYSQL_TYPE_FLOAT:
		case MYSQL_TYPE_DOUBLE:
			return false;
		case MYSQL_TYPE_SET:
		case MYSQL_TYPE_ENUM:
		case MYSQL_TYPE_VARCHAR:
		case MYSQL_TYPE_VAR_STRING:
		case MYSQL_TYPE_STRING:
		case MYSQL_TYPE_JSON:
		case MYSQL_TYPE_NEWDECIMAL:
		case MYSQL_TYPE_DECIMAL:
		case MYSQL_TYPE_TINY_BLOB:
		case MYSQL_TYPE_MEDIUM_BLOB:
		case MYSQL_TYPE_LONG_BLOB:
		case MYSQL_TYPE_BLOB:
			return false;
		case MYSQL_TYPE_BIT:
		case MYSQL_TYPE_GEOMETRY:
			return false;
		case MYSQL_TYPE_TIME:
		case MYSQL_TYPE_TIME2:
			return true;
		case MYSQL_TYPE_DATE:
		case MYSQL_TYPE_NEWDATE:
		case MYSQL_TYPE_DATETIME:
		case MYSQL_TYPE_DATETIME2:
		case MYSQL_TYPE_TIMESTAMP:
		case MYSQL_TYPE_TIMESTAMP2:
			return false;
		}
	}

	alias isDuration = isTime;

	bool isDateTime() const {
		final switch(type_) with (ColumnTypes) {
		case MYSQL_TYPE_NULL:
			return false;
		case MYSQL_TYPE_TINY:
		case MYSQL_TYPE_YEAR:
		case MYSQL_TYPE_SHORT:
		case MYSQL_TYPE_INT24:
		case MYSQL_TYPE_LONG:
		case MYSQL_TYPE_LONGLONG:
		case MYSQL_TYPE_FLOAT:
		case MYSQL_TYPE_DOUBLE:
			return false;
		case MYSQL_TYPE_SET:
		case MYSQL_TYPE_ENUM:
		case MYSQL_TYPE_VARCHAR:
		case MYSQL_TYPE_VAR_STRING:
		case MYSQL_TYPE_STRING:
		case MYSQL_TYPE_JSON:
		case MYSQL_TYPE_NEWDECIMAL:
		case MYSQL_TYPE_DECIMAL:
		case MYSQL_TYPE_TINY_BLOB:
		case MYSQL_TYPE_MEDIUM_BLOB:
		case MYSQL_TYPE_LONG_BLOB:
		case MYSQL_TYPE_BLOB:
			return false;
		case MYSQL_TYPE_BIT:
		case MYSQL_TYPE_GEOMETRY:
			return false;
		case MYSQL_TYPE_TIME:
		case MYSQL_TYPE_TIME2:
			return false;
		case MYSQL_TYPE_DATE:
		case MYSQL_TYPE_NEWDATE:
		case MYSQL_TYPE_DATETIME:
		case MYSQL_TYPE_DATETIME2:
		case MYSQL_TYPE_TIMESTAMP:
		case MYSQL_TYPE_TIMESTAMP2:
			return true;
		}
	}

	alias isTimestamp = isDateTime;

private:
	ColumnTypes type_ = ColumnTypes.MYSQL_TYPE_NULL;
	ubyte sign_;
	ubyte[6] pad_;
	ubyte[BufferSize] buffer_;
	const(char)[] name_;
}


struct MySQLColumn {
	uint length;
	ushort flags;
	ubyte decimals;
	ColumnTypes type;
	const(char)[] name;
}


alias MySQLHeader = MySQLColumn[];


struct MySQLTime {
	uint days;
	ubyte negative;
	ubyte hours;
	ubyte mins;
	ubyte secs;
	uint usecs;

	auto to(T)() const if (is(Unqual!T == Duration)) {
		auto total = days * 86400_000_000L +
			hours * 3600_000_000L +
			mins * 60_000_000L +
			secs * 1_000_000L +
			usecs;
		return cast(T)dur!"usecs"(negative ? -total : total);
	}

	auto to(T)() const if (is(Unqual!T == TimeOfDay)) {
		return cast(T)TimeOfDay(hours, mins, secs);
	}

	static MySQLTime from(Duration duration) {
		MySQLTime time;
		duration.abs.split!("days", "hours", "minutes", "seconds", "usecs")(time.days, time.hours, time.mins, time.secs, time.usecs);
		time.negative = duration.isNegative ? 1 : 0;
		return time;
	}

	static MySQLTime from(TimeOfDay tod) {
		MySQLTime time;
		time.hours = tod.hour;
		time.mins = tod.minute;
		time.secs = tod.second;
		return time;
	}
}

void putMySQLTime(ref OutputPacket packet, in MySQLTime time) {
	if (time.days || time.hours || time.mins || time.mins || time.usecs) {
		auto usecs = time.usecs != 0;
		packet.put!ubyte(usecs ? 12 : 8);
		packet.put!ubyte(time.negative);
		packet.put!uint(time.days);
		packet.put!ubyte(time.hours);
		packet.put!ubyte(time.mins);
		packet.put!ubyte(time.secs);
		if (usecs)
			packet.put!uint(time.usecs);
	} else {
		packet.put!ubyte(0);
	}
}

auto eatMySQLTime(ref InputPacket packet) {
	MySQLTime time;
	switch(packet.eat!ubyte) {
	case 12:
		time.negative = packet.eat!ubyte;
		time.days = packet.eat!uint;
		time.hours = packet.eat!ubyte;
		time.mins = packet.eat!ubyte;
		time.secs = packet.eat!ubyte;
		time.usecs = packet.eat!uint;
		break;
	case 8:
		time.negative = packet.eat!ubyte;
		time.days = packet.eat!uint;
		time.hours = packet.eat!ubyte;
		time.mins = packet.eat!ubyte;
		time.secs = packet.eat!ubyte;
		break;
	case 0:
		break;
	default:
		throw new MySQLProtocolException("Bad time struct format");
	}

	return time;
}


struct MySQLDateTime {
	ushort year;
	ubyte month;
	ubyte day;
	ubyte hour;
	ubyte min;
	ubyte sec;
	uint usec;

	bool valid() const {
		return month != 0;
	}

	T to(T)() const if (is(Unqual!T == SysTime)) {
		assert(valid());
		return cast(T)SysTime(DateTime(year, month, day, hour, min, sec), usec.dur!"usecs", UTC());
	}

	T to(T)() const if (is(Unqual!T == DateTime)) {
		assert(valid());
		return cast(T)DateTime(year, month, day, hour, min, sec);
	}

	T to(T)() const if (is(T == Date)) {
		assert(valid());
		return cast(T)Date(year, month, day);
	}

	T to(T)() const if (is(Unqual!T == TimeOfDay)) {
		return cast(T)TimeOfDay(hour, min, sec);
	}

	static MySQLDateTime from(SysTime sysTime) {
		MySQLDateTime time;

		auto dateTime = cast(DateTime)sysTime;
		time.year = dateTime.year;
		time.month = dateTime.month;
		time.day = dateTime.day;
		time.hour = dateTime.hour;
		time.min = dateTime.minute;
		time.sec = dateTime.second;
		time.usec = cast(int)sysTime.fracSecs.total!"usecs";

		return time;
	}

	static MySQLDateTime from(DateTime dateTime) {
		MySQLDateTime time;

		time.year = dateTime.year;
		time.month = dateTime.month;
		time.day = dateTime.day;
		time.hour = dateTime.hour;
		time.min = dateTime.minute;
		time.sec = dateTime.second;

		return time;
	}

	static MySQLDateTime from(Date date) {
		MySQLDateTime time;

		time.year = date.year;
		time.month = date.month;
		time.day = date.day;

		return time;
	}
}

void putMySQLDateTime(ref OutputPacket packet, in MySQLDateTime time) {
	auto marker = packet.marker!ubyte;
	ubyte length;

	if (time.year || time.month || time.day) {
		length = 4;
		packet.put!ushort(time.year);
		packet.put!ubyte(time.month);
		packet.put!ubyte(time.day);

		if (time.hour || time.min || time.sec || time.usec) {
			length = 7;
			packet.put!ubyte(time.hour);
			packet.put!ubyte(time.min);
			packet.put!ubyte(time.sec);

			if (time.usec) {
				length = 11;
				packet.put!uint(time.usec);
			}
		}
	}

	packet.put!ubyte(marker, length);
}

auto eatMySQLDateTime(ref InputPacket packet) {
	MySQLDateTime time;
	switch(packet.eat!ubyte) {
	case 11:
		time.year = packet.eat!ushort;
		time.month = packet.eat!ubyte;
		time.day = packet.eat!ubyte;
		time.hour = packet.eat!ubyte;
		time.min = packet.eat!ubyte;
		time.sec = packet.eat!ubyte;
		time.usec = packet.eat!uint;
		break;
	case 7:
		time.year = packet.eat!ushort;
		time.month = packet.eat!ubyte;
		time.day = packet.eat!ubyte;
		time.hour = packet.eat!ubyte;
		time.min = packet.eat!ubyte;
		time.sec = packet.eat!ubyte;
		break;
	case 4:
		time.year = packet.eat!ushort;
		time.month = packet.eat!ubyte;
		time.day = packet.eat!ubyte;
		break;
	case 0:
		break;
	default:
		throw new MySQLProtocolException("Bad datetime struct format");
	}

	return time;
}

private void skip(ref const(char)[] x, char ch) {
	if (x.length && (x.ptr[0] == ch)) {
		x = x[1..$];
	} else {
		throw new MySQLProtocolException("Bad datetime string format");
	}
}

auto parseMySQLTime(const(char)[] x) {
	MySQLTime time;

	auto hours = x.parse!int;
	if (hours < 0) {
		time.negative = 1;
		hours = -hours;
	}
	time.days = hours / 24;
	time.hours = cast(ubyte)(hours % 24);
	x.skip(':');
	time.mins = x.parse!ubyte;
	x.skip(':');
	time.secs = x.parse!ubyte;
	if (x.length) {
		x.skip('.');
		time.usecs = x.parse!uint;
		switch (6 - max(6, x.length)) {
		case 0: break;
		case 1: time.usecs *= 10; break;
		case 2: time.usecs *= 100; break;
		case 3: time.usecs *= 1_000; break;
		case 4: time.usecs *= 10_000; break;
		case 5: time.usecs *= 100_000; break;
		default: assert("Bad datetime string format"); break;
		}
	}

	return time;
}

auto parseMySQLDateTime(const(char)[] x) {
	MySQLDateTime time;

	time.year = x.parse!ushort;
	x.skip('-');
	time.month = x.parse!ubyte;
	x.skip('-');
	time.day = x.parse!ubyte;
	if (x.length) {
		x.skip(' ');
		time.hour = x.parse!ubyte;
		x.skip(':');
		time.min = x.parse!ubyte;
		x.skip(':');
		time.sec = x.parse!ubyte;

		if (x.length) {
			x.skip('.');
			time.usec = x.parse!uint;
			switch (6 - max(6, x.length)) {
			case 0: break;
			case 1: time.usec *= 10; break;
			case 2: time.usec *= 100; break;
			case 3: time.usec *= 1_000; break;
			case 4: time.usec *= 10_000; break;
			case 5: time.usec *= 100_000; break;
			default: assert("Bad datetime string format"); break;
			}
		}
	}

	return time;
}

void eatValue(ref InputPacket packet, ref const MySQLColumn column, ref MySQLValue value) {
	auto signed = (column.flags & FieldFlags.UNSIGNED_FLAG) == 0;
	final switch(column.type) with (ColumnTypes) {
	case MYSQL_TYPE_NULL:
		value = MySQLValue(column.name, column.type, signed, null, 0);
		break;
	case MYSQL_TYPE_TINY:
		auto x = packet.eat!ubyte;
		value = MySQLValue(column.name, column.type, signed, &x, 1);
		break;
	case MYSQL_TYPE_YEAR:
	case MYSQL_TYPE_SHORT:
		auto x = packet.eat!ushort;
		value = MySQLValue(column.name, column.type, signed, &x, 2);
		break;
	case MYSQL_TYPE_INT24:
	case MYSQL_TYPE_LONG:
		auto x = packet.eat!uint;
		value = MySQLValue(column.name, column.type, signed, &x, 4);
		break;
	case MYSQL_TYPE_DOUBLE:
	case MYSQL_TYPE_LONGLONG:
		auto x = packet.eat!ulong;
		value = MySQLValue(column.name, column.type, signed, &x, 8);
		break;
	case MYSQL_TYPE_FLOAT:
		auto x = packet.eat!float;
		value = MySQLValue(column.name, column.type, signed, &x, 4);
		break;
	case MYSQL_TYPE_SET:
	case MYSQL_TYPE_ENUM:
	case MYSQL_TYPE_VARCHAR:
	case MYSQL_TYPE_VAR_STRING:
	case MYSQL_TYPE_STRING:
	case MYSQL_TYPE_JSON:
	case MYSQL_TYPE_NEWDECIMAL:
	case MYSQL_TYPE_DECIMAL:
		auto x = packet.eat!(const(char)[])(cast(size_t)packet.eatLenEnc());
		value = MySQLValue(column.name, column.type, signed, &x, typeof(x).sizeof);
		break;
	case MYSQL_TYPE_BIT:
	case MYSQL_TYPE_TINY_BLOB:
	case MYSQL_TYPE_MEDIUM_BLOB:
	case MYSQL_TYPE_LONG_BLOB:
	case MYSQL_TYPE_BLOB:
	case MYSQL_TYPE_GEOMETRY:
		auto x = packet.eat!(const(ubyte)[])(cast(size_t)packet.eatLenEnc());
		value = MySQLValue(column.name, column.type, signed, &x, typeof(x).sizeof);
		break;
	case MYSQL_TYPE_TIME:
	case MYSQL_TYPE_TIME2:
		auto x = eatMySQLTime(packet);
		value = MySQLValue(column.name, column.type, signed, &x, typeof(x).sizeof);
		break;
	case MYSQL_TYPE_DATE:
	case MYSQL_TYPE_NEWDATE:
	case MYSQL_TYPE_DATETIME:
	case MYSQL_TYPE_DATETIME2:
	case MYSQL_TYPE_TIMESTAMP:
	case MYSQL_TYPE_TIMESTAMP2:
		auto x = eatMySQLDateTime(packet);
		value = x.valid() ? MySQLValue(column.name, column.type, signed, &x, typeof(x).sizeof) : MySQLValue(column.name, ColumnTypes.MYSQL_TYPE_NULL, signed, null, 0);
		break;
	}
}

void eatValueText(ref InputPacket packet, ref const MySQLColumn column, ref MySQLValue value) {
	auto signed = (column.flags & FieldFlags.UNSIGNED_FLAG) == 0;
	auto svalue = (column.type != ColumnTypes.MYSQL_TYPE_NULL) ? cast(string)(packet.eat!(const(char)[])(cast(size_t)packet.eatLenEnc())) : string.init;
	final switch(column.type) with (ColumnTypes) {
	case MYSQL_TYPE_NULL:
		value = MySQLValue(column.name, column.type, signed, null, 0);
		break;
	case MYSQL_TYPE_TINY:
		auto x = (svalue.ptr[0] == '-') ? cast(ubyte)(-svalue[1..$].to!byte) : svalue.to!ubyte;
		value = MySQLValue(column.name, column.type, signed, &x, 1);
		break;
	case MYSQL_TYPE_YEAR:
	case MYSQL_TYPE_SHORT:
		auto x = (svalue.ptr[0] == '-') ? cast(ushort)(-svalue[1..$].to!short) : svalue.to!ushort;
		value = MySQLValue(column.name, column.type, signed, &x, 2);
		break;
	case MYSQL_TYPE_INT24:
	case MYSQL_TYPE_LONG:
		auto x = (svalue.ptr[0] == '-') ? cast(uint)(-svalue[1..$].to!int) : svalue.to!uint;
		value = MySQLValue(column.name, column.type, signed, &x, 4);
		break;
	case MYSQL_TYPE_LONGLONG:
		auto x = (svalue.ptr[0] == '-') ? cast(ulong)(-svalue[1..$].to!long) : svalue.to!ulong;
		value = MySQLValue(column.name, column.type, signed, &x, 8);
		break;
	case MYSQL_TYPE_DOUBLE:
		auto x = svalue.to!double;
		value = MySQLValue(column.name, column.type, signed, &x, 8);
		break;
	case MYSQL_TYPE_FLOAT:
		auto x = svalue.to!float;
		value = MySQLValue(column.name, column.type, signed, &x, 4);
		break;
	case MYSQL_TYPE_SET:
	case MYSQL_TYPE_ENUM:
	case MYSQL_TYPE_VARCHAR:
	case MYSQL_TYPE_VAR_STRING:
	case MYSQL_TYPE_STRING:
	case MYSQL_TYPE_JSON:
	case MYSQL_TYPE_NEWDECIMAL:
	case MYSQL_TYPE_DECIMAL:
		value = MySQLValue(column.name, column.type, signed, &svalue, typeof(svalue).sizeof);
		break;
	case MYSQL_TYPE_BIT:
	case MYSQL_TYPE_TINY_BLOB:
	case MYSQL_TYPE_MEDIUM_BLOB:
	case MYSQL_TYPE_LONG_BLOB:
	case MYSQL_TYPE_BLOB:
	case MYSQL_TYPE_GEOMETRY:
		value = MySQLValue(column.name, column.type, signed, &svalue, typeof(svalue).sizeof);
		break;
	case MYSQL_TYPE_TIME:
	case MYSQL_TYPE_TIME2:
		auto x = parseMySQLTime(svalue);
		value = MySQLValue(column.name, column.type, signed, &x, typeof(x).sizeof);
		break;
	case MYSQL_TYPE_DATE:
	case MYSQL_TYPE_NEWDATE:
	case MYSQL_TYPE_DATETIME:
	case MYSQL_TYPE_DATETIME2:
	case MYSQL_TYPE_TIMESTAMP:
	case MYSQL_TYPE_TIMESTAMP2:
		auto x = parseMySQLDateTime(svalue);
		value = x.valid() ? MySQLValue(column.name, column.type, signed, &x, typeof(x).sizeof) : MySQLValue(column.name, ColumnTypes.MYSQL_TYPE_NULL, signed, null, 0);
		break;
	}
}

void putValueType(T)(ref OutputPacket packet, T value) if (is(Unqual!T == Date) || is(Unqual!T == DateTime) || is(Unqual!T == SysTime)) {
	packet.put!ubyte(ColumnTypes.MYSQL_TYPE_TIMESTAMP);
	packet.put!ubyte(0x80);
}

void putValue(T)(ref OutputPacket packet, T value) if (is(Unqual!T == Date) || is(Unqual!T == DateTime) || is(Unqual!T == SysTime)) {
	putMySQLDateTime(packet, MySQLDateTime.from(value));
}

void putValueType(T)(ref OutputPacket packet, T value) if (is(Unqual!T == Duration)) {
	packet.put!ubyte(ColumnTypes.MYSQL_TYPE_TIME);
	packet.put!ubyte(0x00);
}

void putValue(T)(ref OutputPacket packet, T value) if (is(Unqual!T == Duration)) {
	putMySQLTime(packet, MySQLTime.from(value));
}

void putValueType(T)(ref OutputPacket packet, T value) if (isIntegral!T || isBoolean!T) {
	alias UT = Unqual!T;

	enum ubyte sign = isUnsigned!UT ? 0x80 : 0x00;

	static if (is(UT == long) || is(UT == ulong)) {
		packet.put!ubyte(ColumnTypes.MYSQL_TYPE_LONGLONG);
		packet.put!ubyte(sign);
	} else static if (is(UT == int) || is(UT == uint) || is(UT == dchar)) {
		packet.put!ubyte(ColumnTypes.MYSQL_TYPE_LONG);
		packet.put!ubyte(sign);
	} else static if (is(UT == short) || is(UT == ushort) || is(UT == wchar)) {
		packet.put!ubyte(ColumnTypes.MYSQL_TYPE_SHORT);
		packet.put!ubyte(sign);
	} else {
		packet.put!ubyte(ColumnTypes.MYSQL_TYPE_TINY);
		packet.put!ubyte(sign);
	}
}

void putValueType(T)(ref OutputPacket packet, T value) if (isFloatingPoint!T) {
	alias UT = Unqual!T;

	enum ubyte sign = 0x00;

	static if (is(UT == float)) {
		packet.put!ubyte(ColumnTypes.MYSQL_TYPE_FLOAT);
		packet.put!ubyte(sign);
	} else {
		packet.put!ubyte(ColumnTypes.MYSQL_TYPE_DOUBLE);
		packet.put!ubyte(sign);
	}
}

void putValue(T)(ref OutputPacket packet, T value) if (isIntegral!T || isBoolean!T) {
	alias UT = Unqual!T;

	static if (is(UT == long) || is(UT == ulong)) {
		packet.put!ulong(value);
	} else static if (is(UT == int) || is(UT == uint) || is(UT == dchar)) {
		packet.put!uint(value);
	} else static if (is(UT == short) || is(UT == ushort) || is(UT == wchar)) {
		packet.put!ushort(value);
	} else {
		packet.put!ubyte(value);
	}
}

void putValue(T)(ref OutputPacket packet, T value) if (isFloatingPoint!T) {
	alias UT = Unqual!T;

	static if (is(UT == float)) {
		packet.put!float(value);
	} else {
		packet.put!double(cast(double)value);
	}
}

void putValueType(T)(ref OutputPacket packet, T value) if (isSomeString!(OriginalType!T)) {
	packet.put!ubyte(ColumnTypes.MYSQL_TYPE_STRING);
	packet.put!ubyte(0x80);
}

void putValue(T)(ref OutputPacket packet, T value) if (isSomeString!(OriginalType!T)) {
	ulong size = value.length * T.init[0].sizeof;
	packet.putLenEnc(size);
	packet.put(value);
}

void putValueType(T)(ref OutputPacket packet, T value) if (isArray!T && !isSomeString!(OriginalType!T)) {
	foreach(ref item; value)
		putValueType(packet, item);
}

void putValue(T)(ref OutputPacket packet, T value) if (isArray!T && !isSomeString!(OriginalType!T)) {
	foreach(ref item; value)
		putValue(packet, item);
}

void putValueType(T)(ref OutputPacket packet, T value) if (is(Unqual!T == MySQLBinary)) {
	packet.put!ubyte(ColumnTypes.MYSQL_TYPE_BLOB);
	packet.put!ubyte(0x80);
}

void putValue(T)(ref OutputPacket packet, T value) if (is(Unqual!T == MySQLBinary)) {
	ulong size = value.length;
	packet.putLenEnc(size);
	packet.put(value.data);
}

void putValueType(T)(ref OutputPacket packet, T value) if(is(Unqual!T == MySQLValue)) {
	packet.put!ubyte(value.type_);
	packet.put!ubyte(value.sign_);
}

void putValue(T)(ref OutputPacket packet, T value) if (is(Unqual!T == MySQLValue)) {
	final switch(value.type) with (ColumnTypes) {
	case MYSQL_TYPE_NULL:
		break;
	case MYSQL_TYPE_TINY:
		packet.put!ubyte(*cast(ubyte*)value.buffer_.ptr);
		break;
	case MYSQL_TYPE_YEAR:
	case MYSQL_TYPE_SHORT:
		packet.put!ushort(*cast(ushort*)value.buffer_.ptr);
		break;
	case MYSQL_TYPE_INT24:
	case MYSQL_TYPE_LONG:
		packet.put!uint(*cast(uint*)value.buffer_.ptr);
		break;
	case MYSQL_TYPE_LONGLONG:
		packet.put!ulong(*cast(ulong*)value.buffer_.ptr);
		break;
	case MYSQL_TYPE_DOUBLE:
		packet.put!double(*cast(double*)value.buffer_.ptr);
		break;
	case MYSQL_TYPE_FLOAT:
		packet.put!float(*cast(float*)value.buffer_.ptr);
		break;
	case MYSQL_TYPE_SET:
	case MYSQL_TYPE_ENUM:
	case MYSQL_TYPE_VARCHAR:
	case MYSQL_TYPE_VAR_STRING:
	case MYSQL_TYPE_STRING:
	case MYSQL_TYPE_JSON:
	case MYSQL_TYPE_NEWDECIMAL:
	case MYSQL_TYPE_DECIMAL:
	case MYSQL_TYPE_BIT:
	case MYSQL_TYPE_TINY_BLOB:
	case MYSQL_TYPE_MEDIUM_BLOB:
	case MYSQL_TYPE_LONG_BLOB:
	case MYSQL_TYPE_BLOB:
	case MYSQL_TYPE_GEOMETRY:
		packet.putLenEnc((*cast(ubyte[]*)value.buffer_.ptr).length);
		packet.put(*cast(ubyte[]*)value.buffer_.ptr);
		break;
	case MYSQL_TYPE_TIME:
	case MYSQL_TYPE_TIME2:
		packet.putMySQLTime(*cast(MySQLTime*)value.buffer_.ptr);
		break;
	case MYSQL_TYPE_DATE:
	case MYSQL_TYPE_NEWDATE:
	case MYSQL_TYPE_DATETIME:
	case MYSQL_TYPE_DATETIME2:
	case MYSQL_TYPE_TIMESTAMP:
	case MYSQL_TYPE_TIMESTAMP2:
		packet.putMySQLDateTime(*cast(MySQLDateTime*)value.buffer_.ptr);
		break;
	}
}

void putValueType(T)(ref OutputPacket packet, T value) if (is(Unqual!T == typeof(null))) {
	packet.put!ubyte(ColumnTypes.MYSQL_TYPE_NULL);
	packet.put!ubyte(0x00);
}

void putValue(T)(ref OutputPacket packet, T value) if (is(Unqual!T == typeof(null))) {
}

void putValueType(T)(ref OutputPacket packet, T value) if (isInstanceOf!(Nullable, T) || isInstanceOf!(NullableRef, T)) {
	if (value.isNull) {
		putValueType(packet, null);
	} else {
		putValueType(packet, value.get);
	}
}

void putValue(T)(ref OutputPacket packet, T value) if (isInstanceOf!(Nullable, T) || isInstanceOf!(NullableRef, T)) {
	if (value.isNull) {
		putValue(packet, null);
	} else {
		putValue(packet, value.get);
	}
}
