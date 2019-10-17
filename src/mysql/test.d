module src.mysql.test;

//to serve as a basic unit testing for the driver.
// to be able to test you have to provide a connection string in sql_cfg file to any testing mysql database
// the testing code creating a dummy testing database and drops it right after finishing.
unittest {
    import mysql.connection;
    import mysql.socket:VibeSocket;
    import mysql.row;
    import std.datetime:DateTime, Date, SysTime;
    import std.math:fabs;
    import std.stdio:writefln;

    alias DB = Connection!VibeSocket;

	void initTestConnection(ref DB conn) {
		import std.file:readText;
		conn.connect(readText("sql_cfg"));
        conn.execute("create database if not exists MysqlLiteD_testDB");
        conn.use("MysqlLiteD_testDB");
	}

    void finalizeTests(ref DB conn) {
        conn.execute("drop database if exists MysqlLiteD_testDB");
    }

    void testToStruct(ref DB conn) {
        struct A1 {
                int a;
        }

        struct A2 {
                int a, b;
        }

        struct AA {
            int c, d;
        }

        struct A3 {
            int a,b;
            AA aa;
        }

        struct B1 {
            int a;
            float b;
            string c;
        }

        struct B2 {
            int a;
            float b;
            string c;
            DateTime d;
            Date e;
            SysTime f;
        }

        enum E: string {
            INT= "int",
            FLOAT= "float",
            STRING = "string"
        }

        struct B3 {
            int a;
            float b;
            E c;
        }

        writefln("Testing toStruct");

        //prepare testing tables.
        {
            conn.use("MysqlLiteD_testDB");
            conn.execute("drop table if exists A_toStruct");
            conn.execute("drop table if exists B_toStruct");

            conn.execute("create table A_toStruct (id int unsigned not null,
                                        a int,
                                        b int,
                                        c int,
                                        d int)");
            conn.execute("insert into A_toStruct (id, a, b, c, d) values(?, ?, ?, ?, ?)",
                                        1, 10, 20, -30, -40);
            conn.execute("create table B_toStruct (id int unsigned not null,
                                        a int,
                                        b float,
                                        c varchar(200),
                                        d timestamp default current_timestamp,
                                        e date,
                                        f timestamp default current_timestamp)");
            conn.execute("insert into B_toStruct (id, a, b, c, d, e, f) values (?, ?, ?, ?, ?, ?, ?)",
                1, 10, 10.5, "string", DateTime(2013,10,19,17,50,19), Date(2100,1,1),
                SysTime(DateTime(2015,5,19,15,30,0)));
        }

        //test 1
        //very simple struct with int member.
        {
            A1 a;

            conn.execute("select a from A_toStruct where id = 1", (MySQLRow row){
                a = row.toStruct!A1;
            });

            assert(a.a == 10, "test 1 didn't pass");
            writefln(" test 1 successful");
        }

        //test 2
        //testing Strict.yesIgnoreNull should raise exception as rows returned doesn't match.
        {
            bool exception1;

            try {
                A1 a;
                conn.execute("select a, b from A_toStruct where id = 1", (MySQLRow row){
                    a = row.toStruct!(A1, Strict.yesIgnoreNull);
                });
            } catch (MySQLErrorException e) {
                exception1 = true;
            }

            assert(!exception1, "testToStruct test 2 didn't pass");
            writefln(" test 2 successful");
        }

        //test 3
        //testing Strict.yesIgnoreNull should raise exception as rows returned doesn't match.
        {
            bool exception;
            try {
                A2 a;
                conn.execute("select a from A_toStruct where id = 1", (MySQLRow row){
                    a = row.toStruct!(A2, Strict.yesIgnoreNull);
                });
            } catch (MySQLErrorException e) {
                exception = true;
            }

            assert(exception, "testToStruct test 3 didn't pass");
            writefln(" test 3 successful");
        }

        //test 4
        //strict no.
        //(rows returned doesn't match the struct members)
        {
            A1 a;

            try {
                conn.execute("select a, b from A_toStruct where id = 1", (MySQLRow row){
                    a = row.toStruct!(A1, Strict.no);
                });
            } catch (MySQLErrorException e) {
                assert(false, "testToStruct test 4 didn't pass, unexcepted exception ");
            }
            assert(a.a == 10, "testToStruct test 4 didn't pass, a != 10");
            writefln(" test 4 successful");
        }

        //test 5
        //strict no.
        //(rows returned doesn't match the struct members)
        {
            A2 a;
            try {
                conn.execute("select a from A_toStruct where id = 1", (MySQLRow row){
                    a = row.toStruct!(A2, Strict.no);
                });
            } catch (MySQLErrorException e) {
                assert(false, "testToStruct test 5 didn't pass, unexcepted exception");
            }
            assert(a.a == 10, "testToStruct test 5 didn't pass, a != 10");
            writefln(" test 5 successful");
        }

        //test 6
        //nested struct.
        {
            A3 a;

            conn.execute("select a, b, c as `aa.c`, d as `aa.d` from A_toStruct where id = 1", (MySQLRow row){
                a = row.toStruct!A3;
            });
            assert(a.a == 10,    "testToStruct test 6 didn't pass, (a != 10)");
            assert(a.b == 20,    "testToStruct test 6 didn't pass, (b != 20)");
            assert(a.aa.c == -30, "testToStruct test 6 didn't pass, (c != -30)");
            assert(a.aa.d == -40, "testToStruct test 6 didn't pass, (c != -40)");
            writefln(" test 6 successful");
        }

        //test 7
        //struct with different types (int, float, string)
        {
            B1 b;
            conn.execute("select a, b, c from B_toStruct where id = 1",(MySQLRow row){
                b = row.toStruct!B1;
            });
            import std.math:fabs;

            assert(b.a == 10,               "testToStruct test 7 didn't pass (a != 10)");
            assert(fabs(b.b - 10.5) < 0.01, "testToStruct test 7 didn't pass (b !~= 10.5)");
            assert(b.c == "string",         "testToStruct test 7 didn't pass (c != 'string')");
            writefln(" test 7 successful");
        }

        //test 8
        //struct with more different types (int, float, string, DateTime, Date, SysTime)
        {
            B2 b;
            conn.execute("select a, b, c, d, e, f from B_toStruct where id = 1",(MySQLRow row){
                b = row.toStruct!B2;
            });




            assert(b.a == 10,               "testToStruct test 8 didn't pass (a != 10)");
            assert(fabs(b.b - 10.5) < 0.01, "testToStruct test 8 didn't pass (b !~= 10.5)");
            assert(b.c == "string",         "testToStruct test 8 didn't pass (c != 'string')");
            //'2013-10-19 17:50:19'
            assert(b.d == DateTime(2013,10,19,17,50,19), "testToStruct test 8 didn't pass (d != DateTime(2013,10,19,17,50,19))");
            //'2100-01-01'
            assert(b.e == Date(2100,1,1), "testToStruct test 8 didn't pass  (e != Date(2100,1,1))");
            //2015-5-19 15:30:00
            //assert(b.f == SysTime(DateTime(2015,5,19,15,30,0)), "testToStruct test 8 didn't pass  (f != SysTime(DateTime(2015,5,19,15,30,0))");

            writefln(" test 8 successful");
        }

        //test 9
        //struct with enum members
        {
            B3 b;

            conn.execute("select a,b,c from B_toStruct where id = 1", (MySQLRow row){
                b = row.toStruct!B3;
            });
            assert(b.a == 10,               "testToStruct test 9 didn't pass (a != 10)");
            assert(fabs(b.b - 10.5) < 0.01, "testToStruct test 9 didn't pass (b !~= 10.5)");
            assert(b.c == E.STRING,         "testToStruct test 9 didn't pass (c != 'string')");

            writefln(" test 9 successful");
        }
    }

    import mysql.connection:Connection;
	DB conn;
    try {
	    initTestConnection(conn);
        testToStruct(conn);
        finalizeTests(conn);
    } catch(Throwable e) {
        finalizeTests(conn);
        throw e;
    }
}