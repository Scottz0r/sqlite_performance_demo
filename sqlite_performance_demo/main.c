/*
** sqlite_performance_demo - Benchmarks a few ways to INSERT and UPDATE
** data in a SQLite database.
*/
#include "sqlite3.h"
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

/*
** Enable these macros to exeucte PRAGMA commands before running a test function.
*/
// #define PRAGMA_JOURNAL_MEM


#define DB_FILE_PATH "test.db"
#define NUM_EXECUTIONS 10000000
#define RAND_DOUBLE_LIMIT 100.0
#define DASHES "----------------------------------------"


/*
** Enable MEMORY_MODE macro to use SQLite in RAM.
*/
// #define MEMORY_MODE

#if defined MEMORY_MODE
    #define DB_FILE_PATH ":memory:"
#endif

/*
** Structure of test table rows.
*/
typedef struct test_t {
    int64_t rowid;
    char key[25];
    double num1;
    double num2;
    double num3;
    double num4;
} test_t;


/*
** Function prototypes.
*/
void die_db_error();
void open_database();
void close_database();
void setup_test();
void setup_update_test();
void time_test_execution(const char *test_name, void(*fun)(), void(*setup_fun)());
double rand_double();
void insert_rows();
void insert_rows_xact();
void insert_rows_xact_prepared();
void update_rows_pk();
void update_rows_rowid();


/*
** Global instance of the database connection. These tests are not multi-threaded,
** so this won't be an issue.
*/
static sqlite3 *_db;

/*
** Test entry point: Test the various cases.
*/
void main() {
    printf("SQLite Performance Demo\n");
    printf("Testing with %d rows.\n\n", NUM_EXECUTIONS);

    /*
    ** INSERT tests.
    */
    printf("TESTING INSERTS\n");
    printf("%-30s %-15s %-15s\n", "Test", "Time (sec)", "Rows/sec");
    printf("%.30s %.15s %.15s\n", DASHES, DASHES, DASHES);
    //time_test_execution("Insert Rows (no xact)", insert_rows, setup_test); // This will take way to long with higher numbers.
    printf("%30s %15s %15s\n", "Insert Rows (no xact)", "ommitted", "ommitted");
    time_test_execution("Insert Rows (xact)", insert_rows_xact, setup_test);
    time_test_execution("Insert Rows (xact, prep)", insert_rows_xact_prepared, setup_test);

    printf("\n");

    /*
    ** UPDATE tests.
    */
    printf("TESTING UPDATES\n");
    printf("%-30s %-15s %-15s\n", "Test", "Time (sec)", "Rows/sec");
    printf("%.30s %.15s %.15s\n", DASHES, DASHES, DASHES);
    time_test_execution("Update Rows PK", update_rows_pk, setup_update_test);
    time_test_execution("Update Rows ROWID", update_rows_rowid, setup_update_test);

    printf("\n\n");

    printf("Tests completed.\n");
    printf("PRESS ANY KEY TO EXIT\n");
    getchar();
}


/*
** Print a message and exit if there is a critical database error. 
*/
void die_db_error() {
    const char *msg;
    msg = sqlite3_errmsg(_db);
    printf("SQLite Error - %s\n", msg);
    printf("PRESS ANY KEY TO EXIT.\n");
    getchar();
    exit(-1);
}


/*
** Open database file. Deletes any existing database file
** to prevent file growth skewing test results.
*/
void open_database() {
    int rc;

#if !defined MEMORY_MODE
    remove(DB_FILE_PATH);
#endif

    rc = sqlite3_open(DB_FILE_PATH, &_db);
    if (rc != SQLITE_OK) {
        die_db_error(_db);
    }
}


/* Close database file. This will error if there are still
** statement handles open.
*/
void close_database() {
    int rc;
    rc = sqlite3_close(_db);
    if (rc != SQLITE_OK) {
        die_db_error(_db);
    }
}


/*
** Setup the database for testing inserts. This will remove any existing file
** and recreate the test table. The database cannot be open when this runs
** due to file deletion.
*/
void setup_test() {
    int rc;
    const char *sql;

    sql = "CREATE TABLE IF NOT EXISTS Test("
           "key TEXT, "
           "num1 FLOAT, "
           "num2 FLOAT, "
           "num3 FLOAT, "
           "num4 FLOAT, "
           "PRIMARY KEY(key) );";

    rc = sqlite3_exec(_db, sql, NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        die_db_error(_db);
    }
}


/*
** Setup the database for testing updates. Updates will need some dummy data,
** so populate the table with test data from one of the "prior" test cases.
*/
void setup_update_test() {
    setup_test();
    insert_rows_xact_prepared();
}


/*
** The exectuion of the given test function. Takes a function to test and a 
** function to use to prepare the tests.
*/
void time_test_execution(const char *test_name, void(*fun)(), void(*setup_fun)()) {
    time_t start, end;
    double time_ms;

    open_database();

    /*
    ** Setup the test before each run to prevent file size from
    ** skewing results. 
    */
    if (setup_fun != NULL) {
        setup_fun();
    }

#if defined PRAGMA_JOURNAL_MEM
    sqlite3_exec(_db, "PRAGMA journal_mode = MEMORY;", NULL, NULL, NULL);
#endif

    start = clock();
    fun();
    end = clock();

    close_database();

    time_ms = (end - start) / (double)CLOCKS_PER_SEC;
    printf("%30s %12.2f %12.2f\n", test_name, time_ms, NUM_EXECUTIONS / time_ms );
}


/*
** Simple random generator for doubles. 
*/
inline double rand_double() {
    return (double)rand() / (RAND_MAX / RAND_DOUBLE_LIMIT);
}


/* 
** Insert rows using a sprinted SQL statement without a transaction. This 
** is gonna be slow.
*/
void insert_rows() {
    int rc;
    char sql[500];
    char key[25];
    double num1, num2, num3, num4;

    for (int i = 0; i < NUM_EXECUTIONS; ++i) {
        /*
        ** Generate dummy data.
        */
        sprintf(key, "%d", i);
        num1 = rand_double();
        num2 = rand_double();
        num3 = rand_double();
        num4 = rand_double();

        /*
        ** Build SQL for each insert. Note: This is not very safe because single quotes in "key"
        ** could break the statement. Also open for SQL injection and slow due to many sprintfs.
        */
        sprintf(sql, "INSERT INTO Test(key, num1, num2, num3, num4) VALUES('%s', %f, %f, %f, %f);",
            key, num1, num2, num3, num4);

        rc = sqlite3_exec(_db, sql, NULL, NULL, NULL);
        if (rc != SQLITE_OK) {
            die_db_error();
        }
    }

}


/*
** Same setup as inserting, but now wrapped in a transaction. 
*/
void insert_rows_xact() {
    int rc;
    char sql[500];
    char key[25];
    double num1, num2, num3, num4;

    /*
    ** Start the transaction. Changes will be kept in a journal file (or memory)
    ** until the transaction is committed. Will be much faster in bulk processing.
    */
    rc = sqlite3_exec(_db, "BEGIN TRANSACTION;", NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        die_db_error();
    }

    for (int i = 0; i < NUM_EXECUTIONS; ++i) {
        sprintf(key, "K-%d", i);
        num1 = rand_double();
        num2 = rand_double();
        num3 = rand_double();
        num4 = rand_double();

        sprintf(sql, "INSERT INTO Test(key, num1, num2, num3, num4) VALUES('%s', %f, %f, %f, %f);",
            key, num1, num2, num3, num4);

        rc = sqlite3_exec(_db, sql, NULL, NULL, NULL);
        if (rc != SQLITE_OK) {
            die_db_error();
        }
    }

    /*
    ** Commit the transaction, writing changes from the journal into the database.
    */
    rc = sqlite3_exec(_db, "COMMIT TRANSACTION;", NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        die_db_error();
    }
}


/*
** Uses a prepared statement ot insert data instead of building a SQL string
** in each loop. Also uses a transaction. 
*/
void insert_rows_xact_prepared() {
    int rc;
    const char *sql;
    char key[25];
    double num1, num2, num3, num4;
    sqlite3_stmt *stmt;

    rc = sqlite3_exec(_db, "BEGIN TRANSACTION;", NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        die_db_error();
    }

    /*
    ** Build statement once. "?" mark parameters. Also can use other types of parameter
    ** placeholders that will make things easier: https://sqlite.org/c3ref/bind_blob.html
    */
    sql = "INSERT INTO Test(key, num1, num2, num3, num4) VALUES(?, ?, ?, ?, ?);";
    rc = sqlite3_prepare_v3(_db, sql, -1, 0, &stmt, NULL);
    if (rc != SQLITE_OK) {
        die_db_error();
    }

    for (int i = 0; i < NUM_EXECUTIONS; ++i) {
        sprintf(key, "K-%d", i);
        num1 = rand_double();
        num2 = rand_double();
        num3 = rand_double();
        num4 = rand_double();

        /*
        ** Bind parameters. The parameter index starts at 1.
        */
        sqlite3_bind_text(stmt, 1, key, -1, NULL);
        sqlite3_bind_double(stmt, 2, num1);
        sqlite3_bind_double(stmt, 3, num2);
        sqlite3_bind_double(stmt, 4, num3);
        sqlite3_bind_double(stmt, 5, num4);

        rc = sqlite3_step(stmt);
        if (rc != SQLITE_DONE) {
            die_db_error();
        }

        /*
        ** Reset the statement for next execution. Must be done
        ** to reuse prepared statement.
        */
        sqlite3_clear_bindings(stmt);
        sqlite3_reset(stmt);
    }


    rc = sqlite3_exec(_db, "COMMIT TRANSACTION;", NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        die_db_error();
    }

    /*
    ** Must call finalize to cleanup stuff. If this isn't called, then
    ** sqlite3_close(*db) will blow up.
    */
    sqlite3_finalize(stmt);
}


/*
** Updates dummy data using a prepared statement and transaction. This will
** test using a primary key in the update statement.
*/
void update_rows_pk() {
    int rc;
    int up_rc;
    const char *sql;
    const char *tmp_key;
    test_t row;

    sqlite3_stmt *sel_stmt;
    sqlite3_stmt *up_stmt;

    rc = sqlite3_exec(_db, "BEGIN TRANSACTION;", NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        die_db_error();
    }

    sql = "SELECT key, num1, num2, num3, num4 FROM Test ORDER BY key;";
    rc = sqlite3_prepare_v3(_db, sql, -1, 0, &sel_stmt, NULL);
    if (rc != SQLITE_OK) {
        die_db_error();
    }

    sql = "UPDATE Test SET num1 = ?, num2 = ?, num3 = ?, num4 = ? WHERE key = ?;";
    rc = sqlite3_prepare_v3(_db, sql, -1, 0, &up_stmt, NULL);
    if (rc != SQLITE_OK) {
        die_db_error();
    }

    /*
    ** Loop through the rows in the select statement, and update the values using
    ** the primary key.
    */
    while ((rc = sqlite3_step(sel_stmt)) == SQLITE_ROW) {
        /*
        ** Get data from select statement handle. Using a strcpy to get the key
        ** because SQLite will malloc this stuff, so this is probably closer to 
        ** what a "real-life" situation may look like. Once sqlite3_step is called,
        ** the key will be freed.
        */
        tmp_key = (const char *)sqlite3_column_text(sel_stmt, 0);
        strcpy(row.key, tmp_key);

        row.num1 = sqlite3_column_double(sel_stmt, 1);
        row.num2 = sqlite3_column_double(sel_stmt, 2);
        row.num3 = sqlite3_column_double(sel_stmt, 3);
        row.num4 = sqlite3_column_double(sel_stmt, 4);

        /*
        ** Some sort of "update".
        */
        ++row.num1;
        ++row.num2;
        ++row.num3;
        ++row.num4;

        /*
        ** Bind the update columns.
        */
        sqlite3_bind_double(up_stmt, 1, row.num1);
        sqlite3_bind_double(up_stmt, 2, row.num2);
        sqlite3_bind_double(up_stmt, 3, row.num3);
        sqlite3_bind_double(up_stmt, 4, row.num4);
        /*
        ** Bind text. This is a bit different: https://sqlite.org/c3ref/bind_blob.html
        */
        sqlite3_bind_text(up_stmt, 5, row.key, -1, SQLITE_STATIC);

        up_rc = sqlite3_step(up_stmt);
        if (up_rc != SQLITE_DONE) {
            die_db_error();
        }

        /*
        ** Reset update statement for next row.
        */
        sqlite3_reset(up_stmt);
        sqlite3_clear_bindings(up_stmt);
    }

    rc = sqlite3_exec(_db, "COMMIT TRANSACTION;", NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        die_db_error();
    }

    /*
    ** Remember to finalize both statement handles.
    */
    sqlite3_finalize(sel_stmt);
    sqlite3_finalize(up_stmt);
}


/*
** Updates dummy data using a prepared statement and transaction. This will
** test using the built-in ROWID when updating, which should be faster than
** the primary key: https://www.sqlite.org/lang_createtable.html#rowid
*/
void update_rows_rowid() {
    int rc;
    int up_rc;
    const char *sql;
    test_t row;

    sqlite3_stmt *sel_stmt;
    sqlite3_stmt *up_stmt;

    rc = sqlite3_exec(_db, "BEGIN TRANSACTION;", NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        die_db_error();
    }

    /*
    ** "_rowid_" is on every table (unless it has an INTEGER PRIMARY KEY column),
    ** so the below query is valid even though Test table didn't explicitly create
    ** a column called "_rowid_". "rowid" and "oid" are interchangable with "_rowid_".
    */
    sql = "SELECT _rowid_, num1, num2, num3, num4 FROM Test ORDER BY _rowid_;";
    rc = sqlite3_prepare_v3(_db, sql, -1, 0, &sel_stmt, NULL);
    if (rc != SQLITE_OK) {
        die_db_error();
    }

    /*
    ** Same update statement, but using rowid instead.
    */
    sql = "UPDATE Test SET num1 = ?, num2 = ?, num3 = ?, num4 = ? WHERE _rowid_ = ?;";
    rc = sqlite3_prepare_v3(_db, sql, -1, 0, &up_stmt, NULL);
    if (rc != SQLITE_OK) {
        die_db_error();
    }

    /* 
    ** Loop through the rows in the select statement
    */
    while ((rc = sqlite3_step(sel_stmt)) == SQLITE_ROW) {
        row.rowid = sqlite3_column_int64(sel_stmt, 0);
        row.num1 = sqlite3_column_double(sel_stmt, 1);
        row.num2 = sqlite3_column_double(sel_stmt, 2);
        row.num3 = sqlite3_column_double(sel_stmt, 3);
        row.num4 = sqlite3_column_double(sel_stmt, 4);

        /*
        ** Some sort of "update".
        */
        ++row.num1;
        ++row.num2;
        ++row.num3;
        ++row.num4;

        /*
        ** Bind the update columns.
        */
        sqlite3_bind_double(up_stmt, 1, row.num1);
        sqlite3_bind_double(up_stmt, 2, row.num2);
        sqlite3_bind_double(up_stmt, 3, row.num3);
        sqlite3_bind_double(up_stmt, 4, row.num4);
        sqlite3_bind_int64(up_stmt, 5, row.rowid);

        up_rc = sqlite3_step(up_stmt);
        if (up_rc != SQLITE_DONE) {
            die_db_error();
        }

        /*
        ** Reset update statement for next row.
        */
        sqlite3_reset(up_stmt);
        sqlite3_clear_bindings(up_stmt);
    }

    rc = sqlite3_exec(_db, "COMMIT TRANSACTION;", NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        die_db_error();
    }

    /*
    ** Remember to finalize both statement handles.
    */
    sqlite3_finalize(sel_stmt);
    sqlite3_finalize(up_stmt);
}
