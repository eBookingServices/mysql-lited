module mysql.row;


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
        foreach (index, column; header)
            index_[column.name] = index;
    }

    package void set(size_t index, MySQLValue x) {
        values_[index] = x;
    }

    package void nullify(size_t index) {
        values_[index].nullify();
    }

    void structure(T, Strict strict = Strict.yes)(ref T x) if(is(Unqual!T == struct)) {
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

    T structured(T, Strict strict = Strict.yes)() if (is(Unqual!T == struct)) {
        T result;
        structure!(T, strict)(result);
        return result;
    }

    @property length() const {
        return values_.length;
    }

    @property length(size_t x) {
        values_.length = x;
    }

    inout(MySQLValue)* opBinaryRight(string op)(string key) inout if (op == "in") {
        if (auto pindex = key in index_)
            return &values_[*pindex];
        return null;
    }

    const inout(MySQLValue) opIndex(string key) inout {
        if (auto pindex = key in index_)
            return values_[*pindex];
        throw new MySQLErrorException("Column '" ~ key ~ "' was not found in this result set");
    }

    const inout(MySQLValue) opIndex(size_t index) inout {
        return values_[index];
    }

    @property const inout(MySQLValue) opDispatch(string key)() inout {
        return opIndex(key);
    }

    string toString() const {
        import std.conv;
        return to!string(values_);
    }

    string[] toStringArray() const {
        string[] result;
        result.reserve(values_.length);
        foreach(ref value; values_)
            result ~= value.toString;
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
    size_t[string] index_;
}