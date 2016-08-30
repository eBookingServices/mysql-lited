module mysql.row;


import std.algorithm;
import std.datetime;
import std.traits;
import std.typecons;
static import std.ascii;
import std.format : format;

import mysql.exception;
import mysql.type;


private struct IgnoreAttribute {}
private struct OptionalAttribute {}
private struct NameAttribute { const(char)[] name; }
private struct UnCamelCaseAttribute {}


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


template isWritableDataMember(T, string Member) {
	static if (is(TypeTuple!(__traits(getMember, T, Member)))) {
		enum isWritableDataMember = false;
	} else static if (!is(typeof(__traits(getMember, T, Member)))) {
		enum isWritableDataMember = false;
	} else static if (is(typeof(__traits(getMember, T, Member)) == void)) {
		enum isWritableDataMember = false;
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
	@property size_t opDollar() const {
		return values_.length;
	}

	@property const(const(char)[])[] columns() const {
		return names_;
	}

	@property ref auto opDispatch(string key)() const {
		enum hash = hashOf(key);
		if (auto index = find_(hash, key))
			return opIndex(index - 1);
		throw new MySQLErrorException("Column '" ~ key ~ "' was not found in this result set");
	}

	ref auto opIndex(string key) const {
		if (auto index = find_(key.hashOf, key))
			return values_[index - 1];
		throw new MySQLErrorException("Column '" ~ key ~ "' was not found in this result set");
	}

	ref auto opIndex(size_t index) const {
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

package:
	void header_(MySQLHeader header) {
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
		values_.length = headerLen;
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

	uint find_(uint hash, const(char)[] key) const {
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

	ref auto get_(size_t index) {
		return values_[index];
	}

private:
	void structurize(T, Strict strict = Strict.yesIgnoreNull, string path = null)(ref T result) {
		enum unCamel = hasUDA!(T, UnCamelCaseAttribute);

		foreach(member; __traits(allMembers, T)) {
			static if (isWritableDataMember!(T, member)) {
				static if (!hasUDA!(__traits(getMember, result, member), NameAttribute)) {
					enum pathMember = path ~ member;
					static if (unCamel) {
						enum pathMemberAlt = path ~ member.unCamelCase;
					}
				} else {
					enum pathMember = path ~ getUDAs!(__traits(getMember, result, member), NameAttribute)[0].name;
					static if (unCamel) {
						enum pathMemberAlt = pathMember;
					}
				}

				alias MemberType = typeof(__traits(getMember, result, member));

				static if (is(Unqual!MemberType == struct) && !is(Unqual!MemberType == Date) && !is(Unqual!MemberType == DateTime) && !is(Unqual!MemberType == SysTime) && !is(Unqual!MemberType == Duration)) {
					enum pathNew = pathMember ~ ".";
					static if (hasUDA!(__traits(getMember, result, member), OptionalAttribute)) {
						structurize!(MemberType, Strict.no, pathNew)(__traits(getMember, result, member));
					} else {
						structurize!(MemberType, strict, pathNew)(__traits(getMember, result, member));
					}
				} else {
					enum hash = pathMember.hashOf;
					static if (unCamel) {
						enum hashAlt = pathMemberAlt.hashOf;
					}

					auto index = find_(hash, pathMember);
					static if (unCamel && (pathMember != pathMemberAlt)) {
						if (!index)
							index = find_(hashAlt, pathMemberAlt);
					}

					if (index) {
						auto pvalue = values_[index - 1];

						static if ((strict == Strict.no) || (strict == Strict.yesIgnoreNull) || hasUDA!(__traits(getMember, result, member), OptionalAttribute)) {
							if (pvalue.isNull)
								continue;
						}

						__traits(getMember, result, member) = pvalue.get!(Unqual!MemberType);
						continue;
					}

					static if (((strict == Strict.yes) || (strict == Strict.yesIgnoreNull)) && !hasUDA!(__traits(getMember, result, member), OptionalAttribute)) {
						static if (!unCamel || (pathMember == pathMemberAlt)) {
							enum ColumnError = format("Column '%s' was not found in this result set", pathMember);
						} else {
							enum ColumnError = format("Column '%s' or '%s' was not found in this result set", pathMember, pathMember);
						}
						throw new MySQLErrorException(ColumnError);
					}
				}
			}
		}
	}

	MySQLValue[] values_;
	const(char)[][] names_;
	uint[] index_;
}

private string unCamelCase(string x) {
	assert(x.length <= 64);

	enum CharClass {
		LowerCase,
		UpperCase,
		Underscore,
		Digit,
	}

	CharClass classify(char ch) @nogc @safe pure nothrow {
		switch (ch) with (CharClass) {
		case 'A':..case 'Z':
			return UpperCase;
		case 'a':..case 'z':
			return LowerCase;
		case '0':..case '9':
			return Digit;
		case '_':
			return Underscore;
		default:
			assert(false, "only supports identifier-type strings");
		}
	}

	if (x.length > 0) {
		char[128] buffer;
		size_t length;

		auto pcls = classify(x.ptr[0]);
		foreach (i; 0..x.length) with (CharClass) {
			auto ch = x.ptr[i];
			auto cls = classify(ch);

			final switch (cls) {
			case Underscore:
				buffer[length++] = '_';
				break;
			case LowerCase:
				buffer[length++] = ch;
				break;
			case UpperCase:
				if ((pcls != UpperCase) && (pcls != Underscore))
					buffer[length++] = '_';
				buffer[length++] = std.ascii.toLower(ch);
				break;
			case Digit:
				if (pcls != Digit)
					buffer[length++] = '_';
				buffer[length++] = ch;
				break;
			}
			pcls = cls;

			if (length == buffer.length)
				break;
		}
		return buffer[0..length].idup;
	}
	return x;
}


unittest {
	assert("AA".unCamelCase == "aa");
	assert("AaA".unCamelCase == "aa_a");
	assert("AaA1".unCamelCase == "aa_a_1");
	assert("AaA11".unCamelCase == "aa_a_11");
	assert("_AaA1".unCamelCase == "_aa_a_1");
	assert("_AaA11_".unCamelCase == "_aa_a_11_");
	assert("aaA".unCamelCase == "aa_a");
	assert("aaAA".unCamelCase == "aa_aa");
	assert("aaAA1".unCamelCase == "aa_aa_1");
	assert("aaAA11".unCamelCase == "aa_aa_11");
	assert("authorName".unCamelCase == "author_name");
	assert("authorBio".unCamelCase == "author_bio");
	assert("authorPortraitId".unCamelCase == "author_portrait_id");
	assert("authorPortraitID".unCamelCase == "author_portrait_id");
	assert("coverURL".unCamelCase == "cover_url");
	assert("coverImageURL".unCamelCase == "cover_image_url");
}
