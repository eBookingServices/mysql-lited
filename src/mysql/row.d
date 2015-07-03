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


enum Strict {
	yes = 0,
	yesIgnoreNull,
	no,
}


private uint hashOf(const(char)[] x) {
	uint hash = 5381;
	foreach(i; 0..x.length)
		hash = (hash * 33) ^ cast(uint)(std.ascii.toLower(x.ptr[i]));
	return cast(uint)hash;
}

private bool equalsCI(const(char)[]x, const(char)[] y) {
	if (x.length != y.length)
		return false;

	foreach(i; 0..x.length) {
		if (std.ascii.toLower(x.ptr[i]) != std.ascii.toLower(y.ptr[i]))
			return false;
	}

	return true;
}


struct MySQLRow {
	package void header(MySQLHeader header) {
		auto headerLen = header.length;
		auto idealLen = (headerLen + (headerLen >> 2));
		auto indexLen = index_.length;

		index_[] = 0;

		if (indexLen < idealLen) {
			indexLen = max(32, indexLen);

			while (indexLen < idealLen)
				indexLen <<= 1;

			index_.length = indexLen;
		}

		auto mask = (indexLen - 1);
		assert((indexLen & mask) == 0);

		names_.length = headerLen;
		foreach (index, ref column; header) {
			names_[index] = column.name;

			auto hash = hashOf(column.name) & mask;
			auto probe = 1;

			while (true) {
				if (index_[hash] == 0) {
					index_[hash] = cast(uint)index + 1;
					break;
				}

				hash = (hash + probe++) & mask;
			}
		}
	}

	private uint find(uint hash, const(char)[] key) const {
		if (auto mask = index_.length - 1) {
			assert((index_.length & mask) == 0);

			hash = hash & mask;
			auto probe = 1;

			while (true) {
				auto index = index_[hash];
				if (index) {
					if (names_[index - 1].equalsCI(key))
						return index;
					hash = (hash + probe++) & mask;
				} else {
					break;
				}
			}
		}
		return 0;
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

	@property const(const(char)[])[] columns() const {
		return names_;
	}

	@property MySQLValue opDispatch(string key)() const {
		enum hash = hashOf(key);
		if (auto index = find(hash, key))
			return opIndex(index - 1);
		throw new MySQLErrorException("Column '" ~ key ~ "' was not found in this result set");
	}

	MySQLValue opIndex(string key) const {
		if (auto index = find(key.hashOf, key))
			return values_[index - 1];
		throw new MySQLErrorException("Column '" ~ key ~ "' was not found in this result set");
	}

	MySQLValue opIndex(size_t index) const {
		return values_[index];
	}

	const(MySQLValue)* opBinaryRight(string op)(string key) const if (op == "in") {
		if (auto index = find(key.hashOf, key))
			return &values_[index - 1];
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

	int opApply(int delegate(const ref const(char)[], const ref MySQLValue) del) const {
		foreach (size_t i, ref v; values_)
			if (auto ret = del(names_[i], v))
				return ret;
		return 0;
	}

	void toString(Appender)(ref Appender app) const {
		import std.format : formattedWrite;
		formattedWrite(&app, "%s", values_);
	}

	string toString() const {
		import std.conv : to;
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

	void toStruct(T, Strict strict = Strict.yesIgnoreNull)(ref T x) if(is(Unqual!T == struct)) {
		static if (isTuple!(Unqual!T)) {
			foreach(i, ref f; x.field) {
				if (i < length) {
					static if (strict != Strict.yes) {
						if (this[i].isNull)
							continue;
					}

					f = this[i].get!(Unqual!(typeof(f)));
					continue;
				}

				static if ((strict == Strict.yes) || (strict == Strict.yesIgnoreNull)) {
					throw new MySQLErrorException("Column " ~ i ~ " is out of range for this result set");
				}
			}
		} else {
			structurize!(T, strict, null)(x);
		}
	}

	T toStruct(T, Strict strict = Strict.yesIgnoreNull)() if (is(Unqual!T == struct)) {
		T result;
		toStruct!(T, strict)(result);
		return result;
	}

private:
	void structurize(T, Strict strict = Strict.yesIgnoreNull, string path = null)(ref T result) {
		foreach(member; __traits(allMembers, T)) {
			static if (isWritableDataMember!(T, member)) {
				enum pathMember = path ~ member;
				alias MemberType = typeof(__traits(getMember, result, member));

				static if (is(Unqual!MemberType == struct) && !is(Unqual!MemberType == Date) && !is(Unqual!MemberType == DateTime) && !is(Unqual!MemberType == SysTime) && !is(Unqual!MemberType == Duration)) {
					enum pathNew = pathMember ~ ".";
					structurize!(MemberType, strict, pathNew)(__traits(getMember, result, member));
				} else {
					enum hash = pathMember.hashOf;

					if (auto index = find(hash, pathMember)) {
						auto pvalue = values_[index - 1];

						static if ((strict == Strict.no) || (strict == Strict.yesIgnoreNull)) {
							if (pvalue.isNull)
								continue;
						}

						__traits(getMember, result, member) = pvalue.get!(Unqual!MemberType);
						continue;
					}

					static if ((strict == Strict.yes) || (strict == Strict.yesIgnoreNull)) {
						throw new MySQLErrorException("Column '" ~ pathMember ~ "' was not found in this result set");
					}
				}
			}
		}
	}

	MySQLValue[] values_;
	const(char)[][] names_;
	uint[] index_;
}
