module mysql.type;


import std.algorithm;
import std.datetime;
import std.traits;

import mysql.protocol;
import mysql.packet;
import mysql.exception;


struct MySQLValue {
    package enum BufferSize = max(ulong.sizeof, (ulong[]).sizeof, MySQLDateTime.sizeof, MySQLTime.sizeof);
    package this(ColumnTypes type, void* ptr, size_t size) {
        assert(size <= BufferSize);
        type_ = type;
        if (type != ColumnTypes.MYSQL_TYPE_NULL)
            buffer_[0..size] = (cast(ubyte*)ptr)[0..size];
    }

    string toString() const {
        import std.conv;

        final switch(type_) {
            case ColumnTypes.MYSQL_TYPE_NULL:
                return "null";
            case ColumnTypes.MYSQL_TYPE_TINY:
                return to!string(*cast(ubyte*)buffer_.ptr);
            case ColumnTypes.MYSQL_TYPE_YEAR:
            case ColumnTypes.MYSQL_TYPE_SHORT:
                return to!string(*cast(ushort*)buffer_.ptr);
            case ColumnTypes.MYSQL_TYPE_INT24:
            case ColumnTypes.MYSQL_TYPE_LONG:
                return to!string(*cast(uint*)buffer_.ptr);
            case ColumnTypes.MYSQL_TYPE_LONGLONG:
                return to!string(*cast(ulong*)buffer_.ptr);
            case ColumnTypes.MYSQL_TYPE_FLOAT:
                return to!string(*cast(float*)buffer_.ptr);
            case ColumnTypes.MYSQL_TYPE_DOUBLE:
                return to!string(*cast(double*)buffer_.ptr);
            case ColumnTypes.MYSQL_TYPE_SET:
            case ColumnTypes.MYSQL_TYPE_ENUM:
            case ColumnTypes.MYSQL_TYPE_VARCHAR:
            case ColumnTypes.MYSQL_TYPE_VAR_STRING:
            case ColumnTypes.MYSQL_TYPE_STRING:
            case ColumnTypes.MYSQL_TYPE_NEWDECIMAL:
            case ColumnTypes.MYSQL_TYPE_DECIMAL:
                return to!string(*cast(immutable const(char)[]*)buffer_.ptr);
            case ColumnTypes.MYSQL_TYPE_BIT:
            case ColumnTypes.MYSQL_TYPE_TINY_BLOB:
            case ColumnTypes.MYSQL_TYPE_MEDIUM_BLOB:
            case ColumnTypes.MYSQL_TYPE_LONG_BLOB:
            case ColumnTypes.MYSQL_TYPE_BLOB:
            case ColumnTypes.MYSQL_TYPE_GEOMETRY:
                return to!string(*cast(ubyte[]*)buffer_.ptr);
            case ColumnTypes.MYSQL_TYPE_TIME:
            case ColumnTypes.MYSQL_TYPE_TIME2:
                return (*cast(MySQLTime*)buffer_.ptr).toDuration().toString();
            case ColumnTypes.MYSQL_TYPE_DATE:
            case ColumnTypes.MYSQL_TYPE_NEWDATE:
            case ColumnTypes.MYSQL_TYPE_DATETIME:
            case ColumnTypes.MYSQL_TYPE_DATETIME2:
            case ColumnTypes.MYSQL_TYPE_TIMESTAMP:
            case ColumnTypes.MYSQL_TYPE_TIMESTAMP2:
                return (*cast(MySQLDateTime*)buffer_.ptr).to!DateTime().toString();
        }
    }

    T get(T)() const if (isScalarType!T) {
        final switch(type_) {
            case ColumnTypes.MYSQL_TYPE_NULL:
                throw new MySQLErrorException("Cannot convert NULL to scalar");
            case ColumnTypes.MYSQL_TYPE_TINY:
                return cast(T)(*cast(ubyte*)buffer_.ptr);
            case ColumnTypes.MYSQL_TYPE_YEAR:
            case ColumnTypes.MYSQL_TYPE_SHORT:
                return cast(T)(*cast(ushort*)buffer_.ptr);
            case ColumnTypes.MYSQL_TYPE_INT24:
            case ColumnTypes.MYSQL_TYPE_LONG:
                return cast(T)(*cast(uint*)buffer_.ptr);
            case ColumnTypes.MYSQL_TYPE_LONGLONG:
                return cast(T)(*cast(ulong*)buffer_.ptr);
            case ColumnTypes.MYSQL_TYPE_FLOAT:
                return cast(T)(*cast(float*)buffer_.ptr);
            case ColumnTypes.MYSQL_TYPE_DOUBLE:
                return cast(T)(*cast(double*)buffer_.ptr);
            case ColumnTypes.MYSQL_TYPE_SET:
            case ColumnTypes.MYSQL_TYPE_ENUM:
            case ColumnTypes.MYSQL_TYPE_VARCHAR:
            case ColumnTypes.MYSQL_TYPE_VAR_STRING:
            case ColumnTypes.MYSQL_TYPE_STRING:
            case ColumnTypes.MYSQL_TYPE_NEWDECIMAL:
            case ColumnTypes.MYSQL_TYPE_DECIMAL:
                break;
            case ColumnTypes.MYSQL_TYPE_BIT:
            case ColumnTypes.MYSQL_TYPE_TINY_BLOB:
            case ColumnTypes.MYSQL_TYPE_MEDIUM_BLOB:
            case ColumnTypes.MYSQL_TYPE_LONG_BLOB:
            case ColumnTypes.MYSQL_TYPE_BLOB:
            case ColumnTypes.MYSQL_TYPE_GEOMETRY:
                break;
            case ColumnTypes.MYSQL_TYPE_TIME:
            case ColumnTypes.MYSQL_TYPE_TIME2:
                break;
            case ColumnTypes.MYSQL_TYPE_DATE:
            case ColumnTypes.MYSQL_TYPE_NEWDATE:
            case ColumnTypes.MYSQL_TYPE_DATETIME:
            case ColumnTypes.MYSQL_TYPE_DATETIME2:
            case ColumnTypes.MYSQL_TYPE_TIMESTAMP:
            case ColumnTypes.MYSQL_TYPE_TIMESTAMP2:
                break;
        }

        throw new MySQLErrorException("Cannot convert MySQL value to scalar");
    }

    T get(T)() const if (is(T == SysTime) || is(T == DateTime) ||  is(T == Date) || is(T == TimeOfDay)) {
        final switch(type_) {
            case ColumnTypes.MYSQL_TYPE_NULL:
                throw new MySQLErrorException("Cannot convert NULL to timestamp");
            case ColumnTypes.MYSQL_TYPE_TINY:
                break;
            case ColumnTypes.MYSQL_TYPE_YEAR:
                break;
            case ColumnTypes.MYSQL_TYPE_SHORT:
                break;
            case ColumnTypes.MYSQL_TYPE_INT24:
            case ColumnTypes.MYSQL_TYPE_LONG:
                break;
            case ColumnTypes.MYSQL_TYPE_LONGLONG:
                break;
            case ColumnTypes.MYSQL_TYPE_FLOAT:
                break;
            case ColumnTypes.MYSQL_TYPE_DOUBLE:
                break;
            case ColumnTypes.MYSQL_TYPE_SET:
            case ColumnTypes.MYSQL_TYPE_ENUM:
            case ColumnTypes.MYSQL_TYPE_VARCHAR:
            case ColumnTypes.MYSQL_TYPE_VAR_STRING:
            case ColumnTypes.MYSQL_TYPE_STRING:
            case ColumnTypes.MYSQL_TYPE_NEWDECIMAL:
            case ColumnTypes.MYSQL_TYPE_DECIMAL:
                break;
            case ColumnTypes.MYSQL_TYPE_BIT:
            case ColumnTypes.MYSQL_TYPE_TINY_BLOB:
            case ColumnTypes.MYSQL_TYPE_MEDIUM_BLOB:
            case ColumnTypes.MYSQL_TYPE_LONG_BLOB:
            case ColumnTypes.MYSQL_TYPE_BLOB:
            case ColumnTypes.MYSQL_TYPE_GEOMETRY:
                break;
            case ColumnTypes.MYSQL_TYPE_TIME:
            case ColumnTypes.MYSQL_TYPE_TIME2:
                break;
            case ColumnTypes.MYSQL_TYPE_DATE:
            case ColumnTypes.MYSQL_TYPE_NEWDATE:
            case ColumnTypes.MYSQL_TYPE_DATETIME:
            case ColumnTypes.MYSQL_TYPE_DATETIME2:
            case ColumnTypes.MYSQL_TYPE_TIMESTAMP:
            case ColumnTypes.MYSQL_TYPE_TIMESTAMP2:
                return (*cast(MySQLDateTime*)buffer_.ptr).to!T;
        }

        throw new MySQLErrorException("Cannot convert MySQL value to timestamp");
    }

    T get(T)() const if (is(T == Duration)) {
        final switch(type_) {
            case ColumnTypes.MYSQL_TYPE_NULL:
                throw new MySQLErrorException("Cannot convert NULL to time");
            case ColumnTypes.MYSQL_TYPE_TINY:
                break;
            case ColumnTypes.MYSQL_TYPE_YEAR:
                break;
            case ColumnTypes.MYSQL_TYPE_SHORT:
                break;
            case ColumnTypes.MYSQL_TYPE_INT24:
            case ColumnTypes.MYSQL_TYPE_LONG:
                break;
            case ColumnTypes.MYSQL_TYPE_LONGLONG:
                break;
            case ColumnTypes.MYSQL_TYPE_FLOAT:
                break;
            case ColumnTypes.MYSQL_TYPE_DOUBLE:
                break;
            case ColumnTypes.MYSQL_TYPE_SET:
            case ColumnTypes.MYSQL_TYPE_ENUM:
            case ColumnTypes.MYSQL_TYPE_VARCHAR:
            case ColumnTypes.MYSQL_TYPE_VAR_STRING:
            case ColumnTypes.MYSQL_TYPE_STRING:
            case ColumnTypes.MYSQL_TYPE_NEWDECIMAL:
            case ColumnTypes.MYSQL_TYPE_DECIMAL:
                break;
            case ColumnTypes.MYSQL_TYPE_BIT:
            case ColumnTypes.MYSQL_TYPE_TINY_BLOB:
            case ColumnTypes.MYSQL_TYPE_MEDIUM_BLOB:
            case ColumnTypes.MYSQL_TYPE_LONG_BLOB:
            case ColumnTypes.MYSQL_TYPE_BLOB:
            case ColumnTypes.MYSQL_TYPE_GEOMETRY:
                break;
            case ColumnTypes.MYSQL_TYPE_TIME:
            case ColumnTypes.MYSQL_TYPE_TIME2:
                return (*cast(MySQLTime*)buffer_.ptr).toDuration;
            case ColumnTypes.MYSQL_TYPE_DATE:
            case ColumnTypes.MYSQL_TYPE_NEWDATE:
            case ColumnTypes.MYSQL_TYPE_DATETIME:
            case ColumnTypes.MYSQL_TYPE_DATETIME2:
            case ColumnTypes.MYSQL_TYPE_TIMESTAMP:
            case ColumnTypes.MYSQL_TYPE_TIMESTAMP2:
                break;
        }

        throw new MySQLErrorException("Cannot convert MySQL value to time");
    }

    T get(T)() const if (isArray!T) {
        final switch(type_) {
            case ColumnTypes.MYSQL_TYPE_NULL:
                throw new MySQLErrorException("Cannot convert NULL to array");
            case ColumnTypes.MYSQL_TYPE_TINY:
                break;
            case ColumnTypes.MYSQL_TYPE_YEAR:
                break;
            case ColumnTypes.MYSQL_TYPE_SHORT:
                break;
            case ColumnTypes.MYSQL_TYPE_INT24:
            case ColumnTypes.MYSQL_TYPE_LONG:
                break;
            case ColumnTypes.MYSQL_TYPE_LONGLONG:
                break;
            case ColumnTypes.MYSQL_TYPE_FLOAT:
                break;
            case ColumnTypes.MYSQL_TYPE_DOUBLE:
                break;
            case ColumnTypes.MYSQL_TYPE_SET:
            case ColumnTypes.MYSQL_TYPE_ENUM:
            case ColumnTypes.MYSQL_TYPE_VARCHAR:
            case ColumnTypes.MYSQL_TYPE_VAR_STRING:
            case ColumnTypes.MYSQL_TYPE_STRING:
            case ColumnTypes.MYSQL_TYPE_NEWDECIMAL:
            case ColumnTypes.MYSQL_TYPE_DECIMAL:
                return *cast(T*)buffer_.ptr;
            case ColumnTypes.MYSQL_TYPE_BIT:
            case ColumnTypes.MYSQL_TYPE_TINY_BLOB:
            case ColumnTypes.MYSQL_TYPE_MEDIUM_BLOB:
            case ColumnTypes.MYSQL_TYPE_LONG_BLOB:
            case ColumnTypes.MYSQL_TYPE_BLOB:
            case ColumnTypes.MYSQL_TYPE_GEOMETRY:
                return *cast(T*)buffer_.ptr;
            case ColumnTypes.MYSQL_TYPE_TIME:
            case ColumnTypes.MYSQL_TYPE_TIME2:
                break;
            case ColumnTypes.MYSQL_TYPE_DATE:
            case ColumnTypes.MYSQL_TYPE_NEWDATE:
            case ColumnTypes.MYSQL_TYPE_DATETIME:
            case ColumnTypes.MYSQL_TYPE_DATETIME2:
            case ColumnTypes.MYSQL_TYPE_TIMESTAMP:
            case ColumnTypes.MYSQL_TYPE_TIMESTAMP2:
                break;
        }

        throw new MySQLErrorException("Cannot convert MySQL value to array");
    }

    bool isNull() const {
        return type_ == ColumnTypes.MYSQL_TYPE_NULL;
    }

    ColumnTypes type() const {
        return type_;
    }

    package void nullify() {
        type_ = ColumnTypes.MYSQL_TYPE_NULL;
    }

private:
    ColumnTypes type_ = ColumnTypes.MYSQL_TYPE_NULL;
    ubyte[BufferSize] buffer_;
}


struct MySQLColumn {
    uint length;
    ushort flags;
    ubyte decimals;
    ColumnTypes type;
    string name; // todo: fix allocation
}


alias MySQLHeader = MySQLColumn[];
alias MySQLRow = MySQLValue[];


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
        return SysTime(DateTime(year, month, day, hour, min, sec), FracSec.from!"usecs"(usec), UTC());
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
        time.usec = sysTime.fracSec.usecs;

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


MySQLValue eatValue(ref InputPacket packet, in MySQLColumn column) {
    MySQLValue value;

    final switch(column.type) {
        case ColumnTypes.MYSQL_TYPE_NULL:
            value = MySQLValue(column.type, null, 0);
            break;
        case ColumnTypes.MYSQL_TYPE_TINY:
            auto x = packet.eat!ubyte;
            value = MySQLValue(column.type, &x, 1);
            break;
        case ColumnTypes.MYSQL_TYPE_YEAR:
        case ColumnTypes.MYSQL_TYPE_SHORT:
            auto x = packet.eat!ushort;
            value = MySQLValue(column.type, &x, 2);
            break;
        case ColumnTypes.MYSQL_TYPE_INT24:
        case ColumnTypes.MYSQL_TYPE_LONG:
            auto x = packet.eat!uint;
            value = MySQLValue(column.type, &x, 4);
            break;           
        case ColumnTypes.MYSQL_TYPE_DOUBLE:
        case ColumnTypes.MYSQL_TYPE_LONGLONG:
            auto x = packet.eat!ulong;
            value = MySQLValue(column.type, &x, 8);
            break;
        case ColumnTypes.MYSQL_TYPE_FLOAT:
            auto x = packet.eat!float;
            value = MySQLValue(column.type, &x, 4);
            break;
        case ColumnTypes.MYSQL_TYPE_SET:
        case ColumnTypes.MYSQL_TYPE_ENUM:
        case ColumnTypes.MYSQL_TYPE_VARCHAR:
        case ColumnTypes.MYSQL_TYPE_VAR_STRING:
        case ColumnTypes.MYSQL_TYPE_STRING:
        case ColumnTypes.MYSQL_TYPE_NEWDECIMAL:
        case ColumnTypes.MYSQL_TYPE_DECIMAL:
            auto x = packet.eat!(const(char)[])(cast(size_t)packet.eatLenEnc());
            value = MySQLValue(column.type, &x, typeof(x).sizeof);
            break;
        case ColumnTypes.MYSQL_TYPE_BIT:
        case ColumnTypes.MYSQL_TYPE_TINY_BLOB:
        case ColumnTypes.MYSQL_TYPE_MEDIUM_BLOB:
        case ColumnTypes.MYSQL_TYPE_LONG_BLOB:
        case ColumnTypes.MYSQL_TYPE_BLOB:
        case ColumnTypes.MYSQL_TYPE_GEOMETRY:
            auto x = packet.eat!(const(ubyte)[])(cast(size_t)packet.eatLenEnc());
            value = MySQLValue(column.type, &x, typeof(x).sizeof);
            break;
        case ColumnTypes.MYSQL_TYPE_TIME:
        case ColumnTypes.MYSQL_TYPE_TIME2:
            auto x = eatMySQLTime(packet);
            value = MySQLValue(column.type, &x, typeof(x).sizeof);
            break;
        case ColumnTypes.MYSQL_TYPE_DATE:
        case ColumnTypes.MYSQL_TYPE_NEWDATE:
        case ColumnTypes.MYSQL_TYPE_DATETIME:
        case ColumnTypes.MYSQL_TYPE_DATETIME2:
        case ColumnTypes.MYSQL_TYPE_TIMESTAMP:
        case ColumnTypes.MYSQL_TYPE_TIMESTAMP2:
            auto x = eatMySQLDateTime(packet);
            if (x.valid())
                value = MySQLValue(column.type, &x, typeof(x).sizeof);
            else
                value = MySQLValue(ColumnTypes.MYSQL_TYPE_NULL, null, 0);
            break;
    }

    return value;
}

void putValueType(T)(ref OutputPacket packet, T value) if (is(T == Date) || is(T == DateTime) || is(T == SysTime)) {
    packet.put!ubyte(ColumnTypes.MYSQL_TYPE_TIMESTAMP);
    packet.put!ubyte(0x80);
}

void putValue(T)(ref OutputPacket packet, T value) if (is(T == Date) || is(T == DateTime) || is(T == SysTime)) {
    putMySQLDateTime(packet, MySQLDateTime.from(value));
}

void putValueType(T)(ref OutputPacket packet, T value) if (is(T == Duration)) {
    packet.put!ubyte(ColumnTypes.MYSQL_TYPE_TIME);
    packet.put!ubyte(0x00);
}

void putValue(T)(ref OutputPacket packet, T value) if (is(T == Duration)) {
    putMySQLTime(packet, MySQLTime.from(value));
}

void putValueType(T)(ref OutputPacket packet, T value) if (isIntegral!T || isBoolean!T) {
    static if (isUnsigned!T) {
        const ubyte signbyte = 0x80;
    } else {
        const ubyte signbyte = 0x00;
    }

    alias TS = SignedTypeOf!T;

    static if (is(T == long)) {
        packet.put!ubyte(ColumnTypes.MYSQL_TYPE_LONGLONG);
        packet.put!ubyte(signbyte);
    } else static if (is(T == int) || is(T == dchar)) {
        packet.put!ubyte(ColumnTypes.MYSQL_TYPE_LONG);
        packet.put!ubyte(signbyte);
    } else static if (is(T == short) || is(T == wchar)) {
        packet.put!ubyte(ColumnTypes.MYSQL_TYPE_SHORT);
        packet.put!ubyte(signbyte);
    } else static if (is(T == byte) || is(T == char) || is(T == bool)) {
        packet.put!ubyte(ColumnTypes.MYSQL_TYPE_TINY);
        packet.put!ubyte(signbyte);
    }
}

void putValueType(T)(ref OutputPacket packet, T value) if (is(T == typeof(null))) {
    packet.put!ubyte(ColumnTypes.MYSQL_TYPE_NULL);
    packet.put!ubyte(0x00);
}

void putValue(T)(ref OutputPacket packet, T value) if (isIntegral!T || isBoolean!T) {
    alias TS = SignedTypeOf!T;

    static if (is(T == long)) {
        packet.put!ulong(value);
    } else static if (is(T == int) || is(T == dchar)) {
        packet.put!uint(value);
    } else static if (is(T == short) || is(T == wchar)) {
        packet.put!ushort(value);
    } else static if (is(T == byte) || is(T == char) || is(T == bool)) {
        packet.put!ushort(value);
    }
}

void putValueType(T)(ref OutputPacket packet, T value) if (isArray!T) {
    alias ValueType = Unqual!(typeof(T.init[0]));
    static if (isSomeChar!ValueType) {
        packet.put!ubyte(ColumnTypes.MYSQL_TYPE_STRING);
    } else {
        packet.put!ubyte(ColumnTypes.MYSQL_TYPE_BLOB);
    }
    packet.put!ubyte(0x80);
}

void putValue(T)(ref OutputPacket packet, T value) if (isArray!T) {
    alias ValueType = Unqual!(typeof(T.init[0]));

    ulong size = value.length * ValueType.sizeof;
    packet.putLenEnc(size);
    packet.put(value);
}