# mysql-lited
A lightweight native MySQL/MariaDB driver written in D

The goal is a native driver that re-uses the same buffers and the stack as much as possible,
avoiding unnecessary allocations and work for the garbage collector


## notes
- supports all MySQL types with conversion from/to D native types
- results can be  retrieved through a flexible and efficient callback interface
- socket type is a template parameter - currently only a vibesocket is implemented
- only the binary protocol is supported


## example
```d
import std.stdio;
import std.traits : isIntegral;

import mysql;


void usedb() {
	auto client = new MySQLClient("host=sql.moo.com;user=root;pwd=god;db=mew");
	auto conn = client.lockConnection();

	// change database
	conn.use("mewmew");


	// simple insert statement
	conn.execute("insert into users (name, email) values (?, ?)", "frank", "thetank@cowabanga.com");
	auto id = conn.insertID;

	struct User {
		string name;
		string email;
	}

	// simple select statement
	User[] users;
	conn.execute("select name, email from users where id > ?", 13, (MySQLRow row) {
		users ~= row.toStruct!User;
	});


	// batch inserter - inserts in packets of 128k bytes
	auto insert = inserter(conn, "users_copy", "name", "email");
	foreach(user; users)
		insert.row(user.name, user.email);
	insert.flush;


	// re-usable prepared statements
	auto upd = conn.prepare("update users set sequence = ?, login_at = ?, secret = ? where id = ?");
	ubyte[] bytes = [0x4D, 0x49, 0x4C, 0x4B];
	foreach(i; 0..100)
		conn.execute(upd, i, Clock.currTime, MySQLBinary(bytes), i);


	// passing variable or large number of arguments
	string[] names;
	string[] emails;
	int[] ids = [1, 1, 3, 5, 8, 13];
	conn.execute("select name from users where id in " ~ ids.placeholders, ids, (MySQLRow row) {
		writeln(row.name.peek!(char[])); // peek() avoids allocation - cannot use result outside delegate
		names ~= row.name.get!string; // get() duplicates - safe to use result outside delegate
		emails ~= row.email.get!string;
	});


	// another query example
	conn.execute("select id, name, email from users where id > ?", 13, (size_t index /*optional*/, MySQLHeader header /*optional*/, MySQLRow row) {
		writeln(header[0].name, ": ", row.id.get!int);
		return (index < 5); // optionally return false to discard remaining results
	});


	// structured row
	conn.execute("select name, email from users where length(name) > ?", 5, (MySQLRow row) {
		auto user = row.toStruct!User; // default is strict.yesIgnoreNull - a missing field in the row will throw
		// auto user = row.toStruct!(User, Strict.yes); // missing or null will throw
		// auto user = row.toStruct!(User, Strict.no); // missing or null will just be ignored
		writeln(user);
	});
	

	// structured row with nested structs
	struct GeoRef {
		double lat;
		double lng;
	}
	
	struct Place {
		string name;
		GeoRef location;
	}

	conn.execute("select name, lat as `location.lat`, lng as `location.lng` from places", (MySQLRow row) {
		auto place = row.toStruct!Place;
		writeln(place.location);
	});
}
```

## todo
- add proper unit tests
- implement COM\_STMT\_SEND\_LONG\_DATA, and a single parameter binding interface
- make vibe-d dependency optional
