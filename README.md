# mysql-lited
A lightweight native MySQL/MariaDB driver written in D

The goal is a native driver that re-uses the same buffers and the stack as much as possible,
avoiding unnecessary allocations and work for the garbage collector


## notes
- supports all MySQL types with conversion from/to D native types
- results can be  retrieved through a flexible and efficient callback interface
- socket type is a template parameter - currently only a vibesocket is implemented
- both the text and the binary protocol are supported


## example
```d
import std.stdio;

import mysql;


void usedb() {

	// use the default mysql client - uses only prepared statements
	auto client = new MySQLClient("host=sql.moo.com;user=root;pwd=god;db=mew");
	auto conn = client.lockConnection();
	
	
	// use the text protocol instead - instantiate the MySQLClientT template with appropriate arguments
	alias MySQLTextClient = MySQLClientT!(VibeSocket, ConnectionOptions.TextProtocol | ConnectionOptions.TextProtocolCheckNoArgs);
	auto textClient = new MySQLTextClient("host=sql.moo.com;user=root;pwd=god;db=mew");
	auto textConn = textClient.lockConnection();


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

	//struct inserter - insterts struct directly using the field names and UDAs.
	struct Info{
		string employee = "employee";
		int duration_in_months = 12;
	}

	struct InsuranceInfo{
		int number = 50;
		Date started = Date(2015,12,25);
		@sqlignore string description = "insurance description";
		Info info;
	}

	struct BankInfo{
		string iban;
		string name;
		@sqlname("country") string bankCountry;
	}

	struct Client{
		@sqlname("name") string clientName = "default name";
		@sqlname("email") string emailAddress = "default email";
		@sqlname("token") string uniuqeToken = "default token";
		@sqlname("birth_date") Date birthDate = Date(1991, 9, 9); 
		@sqlignore string moreInfoString;	
		InsuranceInfo insurance;
		BankInfo bank;
	}
	
	Client client;
	auto inserter = inserter(conn, "client", "name", "email", "birth_date", "token", "bank.country", "bank.iban", "bank.name" ,
	  "insurance.number", "insurance.started", "insurance.info.employee", "insurance.info.duration_in_months");
	inserter.row(client);

	auto dbClient = db.fetchOne!Client("select * from client limit 1");

	assert(client.serialize == dbClient.serialize)

	//batch insert struct array
	Client[] clients = [Client(), Client(), Client()];
	insert.rows(clients);
	
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

	
	// structured row annotations
	struct PlaceFull {
		uint id;
		string name;
		@optional string thumbnail;	// ok to be null or missing
		@optional GeoRef location;	// nested fields ok to be null or missing
		@optional @as("contact_person") string contact; // optional, and sourced from field contact_person instead

		@ignore File tumbnail;	// completely ignored
	}

	conn.execute("select id, name, thumbnail, lat as `location.lat`, lng as `location.lng`, contact_person from places", (MySQLRow row) {
		auto place = row.toStruct!PlaceFull;
		writeln(place.location);
	});


	// automated struct member uncamelcase
	@uncamel struct PlaceOwner {
		uint placeID;			// matches placeID and place_id
		uint locationId;		// matches locationId and location_id
		string ownerFirstName;	// matches ownerFirstName and owner_first_name
		string ownerLastName;	// matches ownerLastName and owner_last_name
		string feedURL;			// matches feedURL and feed_url
	}
}
```

## todo
- add proper unit tests
- implement COM\_STMT\_SEND\_LONG\_DATA, and a single parameter binding interface
- make vibe-d dependency optional
