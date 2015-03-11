module mysql.row;


import std.algorithm;
import std.datetime;
import std.traits;
import std.typecons;

import mysql.exception;
import mysql.type;


template isWritableDataMember(T, string Member) {
	static if (is(TypeTuple!(__traits(getMember, T, Member)))) {
        enum isWritableDataMember = false;
    } else static if (!is(typeof(__traits(getMember, T, Member)))) {
		enum isWritableDataMember = false;
    } else static if (is(typeof(__traits(getMember, T, Member)) == void)) {
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


enum Strict {
    yes = 0,
    no,
}


struct MySQLRow {
    package void header(MySQLHeader header) {
        index_ = null;
        names_.length = header.length;
        foreach (index, column; header) {
            names_[index] = column.name;
            index_[column.name] = index;
        }
    }

    package void set(size_t index, MySQLValue x) {
        values_[index] = x;
    }

    package void nullify(size_t index) {
        values_[index].nullify();
    }

    package @property length(size_t x) {
        values_.length = x;
    }

    @property length() const {
        return values_.length;
    }

    @property const(string)[] columns() const {
        return names_;
    }

    @property MySQLValue opDispatch(string key)() const {
        return opIndex(key);
    }

    MySQLValue opIndex(string key) const {
        if (auto pindex = key in index_)
            return values_[*pindex];
        throw new MySQLErrorException("Column '" ~ key ~ "' was not found in this result set");
    }

    MySQLValue opIndex(size_t index) const {
        return values_[index];
    }

    const(MySQLValue)* opBinaryRight(string op)(string key) const if (op == "in") {
        if (auto pindex = key in index_)
            return &values_[*pindex];
        return null;
    }

	int opApply(int delegate(const ref MySQLValue value) del) const {
        foreach (ref v; values_)
            if (auto ret = del(v))
                return ret;
        return 0;
    }

	int opApply(int delegate(ref size_t, const ref MySQLValue) del) const {
        foreach (ref size_t i, ref v; values_)
            if (auto ret = del(i, v))
                return ret;
        return 0;
    }

	int opApply(int delegate(const ref string, const ref MySQLValue) del) const {
        foreach (size_t i, ref v; values_)
            if (auto ret = del(names_[i], v))
                return ret;
        return 0;
    }

    string toString() const {
        import std.conv;
        return to!string(values_);
    }

    string[] toStringArray(size_t start = 0, size_t end = ~cast(size_t)0) const {
        end = min(end, values_.length);
        start = min(start, values_.length);
        if (start > end)
            swap(start, end);

        string[] result;
        result.reserve(end - start);
        foreach(i; start..end)
            result ~= values_[i].toString;
        return result;
    }

    void toStruct(T, Strict strict = Strict.yes)(ref T x) if(is(Unqual!T == struct)) {
        static if (isTuple!(Unqual!T)) {
            foreach(i, ref f; x.field) {
                static if (strict == Strict.yes) {
                    f = this[i].get!(Unqual!(typeof(f)));
                } else {
                    if (!this[i].isNull)
                        f = this[i].get!(Unqual!(typeof(f)));
                }
            }
        } else {
            structurize!(T, strict, null)(x);
        }
    }

    T toStruct(T, Strict strict = Strict.yes)() if (is(Unqual!T == struct)) {
        T result;
        toStruct!(T, strict)(result);
        return result;
    }

private:
    void structurize(T, Strict strict = Strict.yes, string path = null)(ref T result) {
        foreach(member; __traits(allMembers, T)) {
            static if (isWritableDataMember!(T, member)) {
                enum pathMember = path ~ member;
                alias MemberType = typeof(__traits(getMember, result, member));

                static if (is(Unqual!MemberType == struct) && !is(Unqual!MemberType == Date) && !is(Unqual!MemberType == DateTime) && !is(Unqual!MemberType == SysTime) && !is(Unqual!MemberType == Duration)) {
                    enum pathNew = pathMember ~ ".";
                    structurize!(MemberType, strict, pathNew)(__traits(getMember, result, member));
                } else {
                    static if (strict == Strict.yes) {
                        __traits(getMember, result, member) = this[pathMember].get!(Unqual!MemberType);
                    } else {
                        auto pvalue = pathMember in this;
                        if (pvalue && !pvalue.isNull)
                            __traits(getMember, result, member) = pvalue.get!(Unqual!MemberType);
                    }
                }
            }
        }
    }

    MySQLValue[] values_;
    string[] names_;
    size_t[string] index_;
}
