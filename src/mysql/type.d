module mysql.type;


import std.algorithm;
import std.datetime;
import std.traits;

import mysql.protocol;
import mysql.packet;
import mysql.exception;
public import mysql.row;


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
	package this(ColumnTypes type, bool signed, void* ptr, size_t size) {
		assert(size <= BufferSize);
		type_ = type;
		sign_ = signed ? 0x00 : 0x80;
		if (type != ColumnTypes.MYSQL_TYPE_NULL)
			buffer_[0..size] = (cast(ubyte*)ptr)[0..size];
	}

	this(T)(T) if (is(Unqual!T == typeof(null))) {
		type_ = ColumnTypes.MYSQL_TYPE_NULL;
		sign_ = 0x00;
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
		(*cast(MySQLDateTime*)buffer_.ptr).from(value);
	}

	this(T)(T value) if (is(Unqual!T == Duration)) {
		type_ = ColumnTypes.MYSQL_TYPE_TIME;
		sign_ = 0x00;
		(*cast(MySQLTime*)buffer_.ptr).from(value);
	}

	this(T)(T value) if (isSomeString!T) {
		static assert(typeof(T.init[0]).sizeof == 1, "Unsupported string type: " ~ T);

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
		import std.format : formattedWrite;

		final switch(type_) with (ColumnTypes) {
			case MYSQL_TYPE_NULL:
				break;
			case MYSQL_TYPE_TINY:
				formattedWrite(&app, "%d", *cast(ubyte*)buffer_.ptr);
				break;
			case MYSQL_TYPE_YEAR:
			case MYSQL_TYPE_SHORT:
				formattedWrite(&app, "%d", *cast(ushort*)buffer_.ptr);
				break;
			case MYSQL_TYPE_INT24:
			case MYSQL_TYPE_LONG:
				formattedWrite(&app, "%d", *cast(uint*)buffer_.ptr);
				break;
			case MYSQL_TYPE_LONGLONG:
				formattedWrite(&app, "%d", *cast(ulong*)buffer_.ptr);
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
				formattedWrite(&app, "%s", (*cast(MySQLTime*)buffer_.ptr).toDuration());
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
		import std.conv : to;

		final switch(type_) with (ColumnTypes) {
			case MYSQL_TYPE_NULL:
				return null;
			case MYSQL_TYPE_TINY:
				return to!string(*cast(ubyte*)buffer_.ptr);
			case MYSQL_TYPE_YEAR:
			case MYSQL_TYPE_SHORT:
				return to!string(*cast(ushort*)buffer_.ptr);
			case MYSQL_TYPE_INT24:
			case MYSQL_TYPE_LONG:
				return to!string(*cast(uint*)buffer_.ptr);
			case MYSQL_TYPE_LONGLONG:
				return to!string(*cast(ulong*)buffer_.ptr);
			case MYSQL_TYPE_FLOAT:
				return to!string(*cast(float*)buffer_.ptr);
			case MYSQL_TYPE_DOUBLE:
				return to!string(*cast(double*)buffer_.ptr);
			case MYSQL_TYPE_SET:
			case MYSQL_TYPE_ENUM:
			case MYSQL_TYPE_VARCHAR:
			case MYSQL_TYPE_VAR_STRING:
			case MYSQL_TYPE_STRING:
			case MYSQL_TYPE_NEWDECIMAL:
			case MYSQL_TYPE_DECIMAL:
			case MYSQL_TYPE_TINY_BLOB:
			case MYSQL_TYPE_MEDIUM_BLOB:
			case MYSQL_TYPE_LONG_BLOB:
			case MYSQL_TYPE_BLOB:
				return (*cast(string*)buffer_.ptr).idup;
			case MYSQL_TYPE_BIT:
			case MYSQL_TYPE_GEOMETRY:
				return to!string(*cast(ubyte[]*)buffer_.ptr);
			case MYSQL_TYPE_TIME:
			case MYSQL_TYPE_TIME2:
				return (*cast(MySQLTime*)buffer_.ptr).toDuration().toString();
			case MYSQL_TYPE_DATE:
			case MYSQL_TYPE_NEWDATE:
			case MYSQL_TYPE_DATETIME:
			case MYSQL_TYPE_DATETIME2:
			case MYSQL_TYPE_TIMESTAMP:
			case MYSQL_TYPE_TIMESTAMP2:
				return (*cast(MySQLDateTime*)buffer_.ptr).to!DateTime().toString();
		}
	}
	
	T get(T)(lazy T def) const {
		return !isNull ? get!T : def;
	}

	T get(T)() const if (isScalarType!T) {
		switch(type_) with (ColumnTypes) {
			case MYSQL_TYPE_NULL:
				throw new MySQLErrorException("Cannot convert NULL to scalar");
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
				throw new MySQLErrorException("Cannot convert MySQL value to scalar");
		}
	}

	T get(T)() const if (is(Unqual!T == SysTime) || is(Unqual!T == DateTime) ||  is(Unqual!T == Date) || is(Unqual!T == TimeOfDay)) {
		switch(type_) with (ColumnTypes) {
			case MYSQL_TYPE_NULL:
				throw new MySQLErrorException("Cannot convert NULL to timestamp");
			case MYSQL_TYPE_DATE:
			case MYSQL_TYPE_NEWDATE:
			case MYSQL_TYPE_DATETIME:
			case MYSQL_TYPE_DATETIME2:
			case MYSQL_TYPE_TIMESTAMP:
			case MYSQL_TYPE_TIMESTAMP2:
				return (*cast(MySQLDateTime*)buffer_.ptr).to!T;
			default:
				throw new MySQLErrorException("Cannot convert MySQL value to timestamp");
		}
	}

	T get(T)() const if (is(Unqual!T == Duration)) {
		switch(type_) with (ColumnTypes) {
			case MYSQL_TYPE_NULL:
				throw new MySQLErrorException("Cannot convert NULL to time");
			case MYSQL_TYPE_TIME:
			case MYSQL_TYPE_TIME2:
				return (*cast(MySQLTime*)buffer_.ptr).toDuration;
			default:
				throw new MySQLErrorException("Cannot convert MySQL value to time");
		}
	}

	T get(T)() const if (isArray!T) {
		switch(type_) with (ColumnTypes) {
			case MYSQL_TYPE_NULL:
				throw new MySQLErrorException("Cannot convert NULL to array");
			case MYSQL_TYPE_SET:
			case MYSQL_TYPE_ENUM:
			case MYSQL_TYPE_VARCHAR:
			case MYSQL_TYPE_VAR_STRING:
			case MYSQL_TYPE_STRING:
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
				throw new MySQLErrorException("Cannot convert MySQL value to array");
		}
	}

	T peek(T)() const if (isScalarType!T) {
		return get!T;
	}

	T peek(T)() const if (is(Unqual!T == SysTime) || is(Unqual!T == DateTime) ||  is(Unqual!T == Date) || is(Unqual!T == TimeOfDay)) {
		return get!T;
	}

	T peek(T)() const if (is(Unqual!T == Duration)) {
		return get!T;
	}

	T peek(T)() const if (isArray!T) {
		switch(type_) with (ColumnTypes) {
			case MYSQL_TYPE_NULL:
				throw new MySQLErrorException("Cannot convert NULL to array");
			case MYSQL_TYPE_SET:
			case MYSQL_TYPE_ENUM:
			case MYSQL_TYPE_VARCHAR:
			case MYSQL_TYPE_VAR_STRING:
			case MYSQL_TYPE_STRING:
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
				throw new MySQLErrorException("Cannot convert MySQL value to array");
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

	package void nullify() {
		type_ = ColumnTypes.MYSQL_TYPE_NULL;
	}

private:
	ColumnTypes type_ = ColumnTypes.MYSQL_TYPE_NULL;
	ubyte sign_;
	ubyte[6] pad_;
	ubyte[BufferSize] buffer_;
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

	Duration toDuration() {
		auto total = days * 86400_000_000L +
			hours * 3600_000_000L +
			mins * 60_000_000L +
			secs * 1_000_000L +
			usecs;
		return dur!"usecs"(negative ? -total : total);
	}

	static MySQLTime from(Duration duration) {
		MySQLTime time;
		duration.abs.split!("days", "hours", "minutes", "seconds", "usecs")(time.days, time.hours, time.mins, time.secs, time.usecs);
		time.negative = duration.isNegative ? 1 : 0;
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
	ushort year = 0;
	ubyte month = 0;
	ubyte day = 0;
	ubyte hour = 0;
	ubyte min = 0;
	ubyte sec = 0;
	uint usec = 0;

	bool valid() const {
		return month != 0;
	}

	T to(T)() if (is(T == SysTime)) {
		return SysTime(DateTime(year, month, day, hour, min, sec), usec.dur!"usecs", UTC());
	}

	T to(T)() if (is(T == DateTime)) {
		return DateTime(year, month, day, hour, min, sec);
	}

	T to(T)() if (is(T == Date)) {
		return Date(year, month, day);
	}

	T to(T)() if (is(T == TimeOfDay)) {
		return TimeOfDay(hour, min, sec);
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
	ubyte length = 0;

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


MySQLValue eatValue(ref InputPacket packet, ref const MySQLColumn column) {
	MySQLValue value;

	// todo: avoid unnecessary copying packet->stack->value - copy directly packet->value
	auto signed = (column.flags & FieldFlags.UNSIGNED_FLAG) == 0;
	final switch(column.type) with (ColumnTypes) {
		case MYSQL_TYPE_NULL:
			value = MySQLValue(column.type, signed, null, 0);
			break;
		case MYSQL_TYPE_TINY:
			auto x = packet.eat!ubyte;
			value = MySQLValue(column.type, signed, &x, 1);
			break;
		case MYSQL_TYPE_YEAR:
		case MYSQL_TYPE_SHORT:
			auto x = packet.eat!ushort;
			value = MySQLValue(column.type, signed, &x, 2);
			break;
		case MYSQL_TYPE_INT24:
		case MYSQL_TYPE_LONG:
			auto x = packet.eat!uint;
			value = MySQLValue(column.type, signed, &x, 4);
			break;
		case MYSQL_TYPE_DOUBLE:
		case MYSQL_TYPE_LONGLONG:
			auto x = packet.eat!ulong;
			value = MySQLValue(column.type, signed, &x, 8);
			break;
		case MYSQL_TYPE_FLOAT:
			auto x = packet.eat!float;
			value = MySQLValue(column.type, signed, &x, 4);
			break;
		case MYSQL_TYPE_SET:
		case MYSQL_TYPE_ENUM:
		case MYSQL_TYPE_VARCHAR:
		case MYSQL_TYPE_VAR_STRING:
		case MYSQL_TYPE_STRING:
		case MYSQL_TYPE_NEWDECIMAL:
		case MYSQL_TYPE_DECIMAL:
			auto x = packet.eat!(const(char)[])(cast(size_t)packet.eatLenEnc());
			value = MySQLValue(column.type, signed, &x, typeof(x).sizeof);
			break;
		case MYSQL_TYPE_BIT:
		case MYSQL_TYPE_TINY_BLOB:
		case MYSQL_TYPE_MEDIUM_BLOB:
		case MYSQL_TYPE_LONG_BLOB:
		case MYSQL_TYPE_BLOB:
		case MYSQL_TYPE_GEOMETRY:
			auto x = packet.eat!(const(ubyte)[])(cast(size_t)packet.eatLenEnc());
			value = MySQLValue(column.type, signed, &x, typeof(x).sizeof);
			break;
		case MYSQL_TYPE_TIME:
		case MYSQL_TYPE_TIME2:
			auto x = eatMySQLTime(packet);
			value = MySQLValue(column.type, signed, &x, typeof(x).sizeof);
			break;
		case MYSQL_TYPE_DATE:
		case MYSQL_TYPE_NEWDATE:
		case MYSQL_TYPE_DATETIME:
		case MYSQL_TYPE_DATETIME2:
		case MYSQL_TYPE_TIMESTAMP:
		case MYSQL_TYPE_TIMESTAMP2:
			auto x = eatMySQLDateTime(packet);
			if (x.valid())
				value = MySQLValue(column.type, signed, &x, typeof(x).sizeof);
			else
				value = MySQLValue(ColumnTypes.MYSQL_TYPE_NULL, signed, null, 0);
			break;
	}

	return value;
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

void putValueType(T)(ref OutputPacket packet, T value) if (is(Unqual!T == typeof(null))) {
	packet.put!ubyte(ColumnTypes.MYSQL_TYPE_NULL);
	packet.put!ubyte(0x00);
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

void putValueType(T)(ref OutputPacket packet, T value) if (isSomeString!T) {
	packet.put!ubyte(ColumnTypes.MYSQL_TYPE_STRING);
	packet.put!ubyte(0x80);
}

void putValue(T)(ref OutputPacket packet, T value) if (isSomeString!T) {
	ulong size = value.length * T.init[0].sizeof;
	packet.putLenEnc(size);
	packet.put(value);
}

void putValueType(T)(ref OutputPacket packet, T value) if (isArray!T && !isSomeString!T) {
	foreach(ref item; value)
		putValueType(packet, item);
}

void putValue(T)(ref OutputPacket packet, T value) if (isArray!T && !isSomeString!T) {
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
			packet.put!double(*cast(float*)value.buffer_.ptr);
			break;
		case MYSQL_TYPE_SET:
		case MYSQL_TYPE_ENUM:
		case MYSQL_TYPE_VARCHAR:
		case MYSQL_TYPE_VAR_STRING:
		case MYSQL_TYPE_STRING:
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
