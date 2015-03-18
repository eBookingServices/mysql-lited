module mysql.packet;


import std.algorithm;
import std.traits;

import mysql.exception;


struct InputPacket {
    @disable this();

    this(ubyte[]* buffer) {
        buffer_ = buffer;
        in_ = *buffer_;
    }

    T peek(T)() if (!isArray!T) {
        assert(T.sizeof <= in_.length);
        return *(cast(T*)in_.ptr);
    }

    T eat(T)() if (!isArray!T) {
        assert(T.sizeof <= in_.length);
        auto ptr = cast(T*)in_.ptr;
        in_ = in_[T.sizeof..$];
        return *ptr;
    }

    T peek(T)(size_t count) if (isArray!T) {
        alias ValueType = typeof(Type.init[0]);

        assert(ValueType.sizeof * count <= in_.length);
        auto ptr = cast(ValueType*)in_.ptr;
        return ptr[0..count];
    }

    T eat(T)(size_t count) if (isArray!T) {
        alias ValueType = typeof(T.init[0]);

        assert(ValueType.sizeof * count <= in_.length);
        auto ptr = cast(ValueType*)in_.ptr;
        in_ = in_[ValueType.sizeof * count..$];
        return ptr[0..count];
    }

    void expect(T)(T x) {
        if (x != eat!T)
            throw new MySQLProtocolException("Bad packet format");
    }

    void skip(size_t count) {
        assert(count <= in_.length);
        in_ = in_[count..$];
    }

    auto countUntil(ubyte x, bool expect) {
        auto index = in_.countUntil(x);
        if (expect) {
            if ((index < 0) || (in_[index] != x))
                throw new MySQLProtocolException("Bad packet format");
        }
        return index;
    }

    ulong eatLenEnc() {
        auto header = eat!ubyte;
        if (header < 0xfb)
            return header;

        ulong lo;
        ulong hi;

        switch(header) {
        case 0xfb:
            return 0;
        case 0xfc:
            return eat!ushort;
        case 0xfd:
            lo = eat!ubyte;
            hi = eat!ushort;
            return lo | (hi << 8);
        case 0xfe:
            lo = eat!uint;
            hi = eat!uint;
            return lo | (hi << 32);
        default:
            throw new MySQLProtocolException("Bad packet format");
        }
    }

    auto remaining() const {
        return in_.length;
    }

    bool empty() const {
        return in_.length == 0;
    }
protected:
    ubyte[]* buffer_;
    ubyte[] in_;
}


struct OutputPacket {
    @disable this();

    this(ubyte[]* buffer) {
        buffer_ = buffer;
        out_ = buffer_.ptr + 4;
    }

    void put(T)(T x) if (!isArray!T) {
        put(offset_, x);
    }

    void put(T)(T x) if (isArray!T) {
        put(offset_, x);
    }

    void put(T)(size_t offset, T x) if (!isArray!T) {
        grow(offset, T.sizeof);

        *(cast(T*)(out_ + offset)) = x;
        offset_ = max(offset + T.sizeof, offset_);
    }

    void put(T)(size_t offset, T x) if (isArray!T) {
        alias ValueType = Unqual!(typeof(T.init[0]));

        grow(offset, ValueType.sizeof * x.length);

        (cast(ValueType*)(out_ + offset))[0..x.length] = x;
        offset_ = max(offset + (ValueType.sizeof * x.length), offset_);
    }

    void putLenEnc(ulong x) {
        if (x < 0xfb) {
            put!ubyte(cast(ubyte)x);
        } else if (x <= ushort.max) {
            put!ubyte(0xfc);
            put!ushort(cast(ushort)x);
        } else if (x <= (uint.max >> 8)) {
            put!ubyte(0xfd);
            put!ubyte(cast(ubyte)(x));
            put!ushort(cast(ushort)(x >> 8));
        } else {
            put!ubyte(0xfe);
            put!uint(cast(uint)x);
            put!uint(cast(uint)(x >> 32));
        }
    }

    size_t marker(T)() if (!isArray!T) {
        grow(offset_, T.sizeof);

        auto place = offset_;
        offset_ += T.sizeof;
        return place;
    }

    size_t marker(T)(size_t count) if (isArray!T) {
        alias ValueType = Unqual!(typeof(T.init[0]));
        grow(offset_, ValueType.sizeof * x.length);

        auto place = offset_;
        offset_ += (ValueType.sizeof * x.length);
        return place;
    }

    void finalize(ubyte seq) {
        if (offset_ >=  0xffffff)
            throw new MySQLConnectionException("Packet size exceeds 2^24");
        uint length = cast(uint)offset_;
        uint header = cast(uint)((offset_ & 0xffffff) | (seq << 24));
        *(cast(uint*)buffer_.ptr) = header;
    }

    void finalize(ubyte seq, size_t extra) {
        if (offset_ + extra >= 0xffffff)
            throw new MySQLConnectionException("Packet size exceeds 2^24");
        uint length = cast(uint)(offset_ + extra);
        uint header = cast(uint)((length & 0xffffff) | (seq << 24));
        *(cast(uint*)buffer_.ptr) = header;
    }

    void reset() {
        offset_ = 0;
    }
    void reserve(size_t size) {
        (*buffer_).length = max((*buffer_).length, 4 + size);
        out_ = buffer_.ptr + 4;
    }

    void fill(ubyte x, size_t size) {
        grow(offset_, size);
        out_[offset_..offset_ + size] = 0;
        offset_ += size;
    }

    size_t length() const {
        return offset_;
    }

    bool empty() const {
        return offset_ == 0;
    }

    const(ubyte)[] get() const {
        return (*buffer_)[0..4 + offset_];
    }
protected:
    void grow(size_t offset, size_t size) {
        auto requested = 4 + offset + size;
        if (requested > buffer_.length) {
            auto capacity = (*buffer_).capacity;
            while (capacity < requested)
                capacity <<= 1;
            buffer_.length = requested;
            out_ = buffer_.ptr + 4;
        }
    }
    ubyte[]* buffer_;
    ubyte* out_;
    size_t offset_ = 0;
}