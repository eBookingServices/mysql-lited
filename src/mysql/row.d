module mysql.row;


import std.traits;
import std.typecons;

import mysql.exception;
import mysql.type;


template isPublicDataMember(T, string Member) {
    static if (!__traits(compiles, typeof(__traits(getMember, T, Member)))) {
		enum isPublicDataMember = false;
    } else {
        enum isPublicDataMember = !isSomeFunction!(typeof(__traits(getMember, T, Member)));
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

    T structured(T, Strict strict = Strict.yes)() if(is(Unqual!T == struct)) {
        T result;
        static if (isTuple!(Unqual!T)) {
            foreach(i, ref f; result.field)
                f = row[i].get!(Unqual!(typeof(f)));
        } else {
            foreach(member; __traits(allMembers, T)) {
                static if (isPublicDataMember!(T, member)) {
                    static if (strict == Strict.yes) {
                        __traits(getMember, result, member) = this[member].get!(Unqual!(typeof(__traits(getMember, result, member))));
                    } else {
                        auto pvalue = member in this;
                        if (pvalue && !pvalue.isNull) {
                            __traits(getMember, result, member) = this[member].get!(Unqual!(typeof(__traits(getMember, result, member))));
                        }
                    }
                }
            }
        }

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

    string toString() {
        import std.conv;
        return to!string(values_);
    }
private:
    MySQLValue[] values_;
    size_t[string] index_;
}