# mysql-lited
A lightweight native mysql driver written in D

The goal is a native driver that re-uses the same buffers and the stack as much as possible, avoiding unnecessary work for the garbage collector


## notes
- supports all MySQL types with conversion from/to D types
- results are retrieved through a flexible callback interface
- socket type is a template parameter - currently only a vibesocket is implemented - more to come
- only the binary protocol is supported


## example
```d
conn.connect("host=sql.moo.com;user=root;pwd=god;db=mew");
//conn.use("mewmew");

// re-usable prepared statements
auto upd = conn.prepare("update manytypes set int_ = ?, timestamp_ = ?, blob_ = ?");
foreach(int_; 0..100) {
    conn.execute(upd, int_, Clock.currTime, sha1Of(int_));
}

// one-shot query
conn.execute("select * from manytypes where id > ?", 13, (size_t index /*optional*/, MySQLHeader header /*optional*/, MySQLRow row) {
    writeln(header[0].name, ": ", row.int_.get!int);
    if (index == 5)
        return false; // optionally return false to discard remaining results
});

// structured row
struct Point {
    int x, y, z;
};

conn.execute("select x, y, z from points where x > y and y > z", (MySQLRow row) {
    auto p = row.structured!Point; // default is strict mode, where a missing or null field in the row will throw
    // auto p = row.structured!(Point, Strict.no); // missing or null will just be ignored
    writeln(p);
});
```

## todo
- add proper unit tests
- implement COM_STMT_SEND_LONG_DATA, and a single parameter binding interface
- make vibe-d dependency optional
