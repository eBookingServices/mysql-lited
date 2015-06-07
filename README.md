# mysql-lited
A lightweight native mysql driver written in D

The goal is a native driver that re-uses the same buffers and the stack as much as possible,
avoiding unnecessary allocations and work for the garbage collector


## notes
- supports all MySQL types with conversion from/to D types
- results are retrieved through a flexible callback interface
- socket type is a template parameter - currently only a vibesocket is implemented
- only the binary protocol is supported


## example
```d
import std.stdio;
import std.traits : isIntegral;

import mysql;

string placeholders(T)(T x) if (isIntegral!T) {
	import std.range : repeat, take;
	return "(" ~ ("?".repeat().take(x).join(",")) ~ ")";
}

void usedb() {
	auto client = new MySQLClient("host=sql.moo.com;user=root;pwd=god;db=mew");
	auto conn = client.lockConnection();

	// change database
	conn.use("mewmew");

	// re-usable prepared statements
	auto upd = conn.prepare("update manytypes set int_ = ?, timestamp_ = ?, blob_ = ?");
	ubyte[] bytes = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
	foreach(int_; 0..100) {
		conn.execute(upd, int_, Clock.currTime, MySQLBinary(bytes));
	}

	// passing variable or large number of arguments
	int[] ids = [1, 1, 3, 5, 8, 13];
	string[] names;
	conn.execute("select name from items where id in " ~ ids.length.placeholders, ids, (MySQLRow row) {
		writeln(row.name.peek!(char[])); // peek() avoids allocation - cannot use result outside delegate
		names ~= row.name.get!string; // get() duplicates - safe to use result outside delegate
	});

	// one-shot query
	conn.execute("select * from manytypes where id > ?", 13, (size_t index /*optional*/, MySQLHeader header /*optional*/, MySQLRow row) {
		writeln(header[0].name, ": ", row.int_.get!int);
		return (index < 5); // optionally return false to discard remaining results
	});

	// structured row
	struct Point {
		int x, y, z;
	}

	conn.execute("select x, y, z from points where x > y and y > z", (MySQLRow row) {
		auto p = row.toStruct!Point; // default is strict.yesIgnoreNull - a missing field in the row will throw
		// auto p = row.toStruct!(Point, Strict.yes); // missing or null will throw
		// auto p = row.toStruct!(Point, Strict.no); // missing or null will just be ignored
		writeln(p);
	});
}
```

## todo
- add proper unit tests
- implement COM\_STMT\_SEND\_LONG\_DATA, and a single parameter binding interface
- make vibe-d dependency optional
