use crate::common::{limbo_exec_rows, limbo_exec_rows_error, sqlite_exec_rows, TempDatabase};
use rusqlite::params;

#[test]
fn test_create_without_rowid_table() -> anyhow::Result<()> {
    let db = TempDatabase::new_empty();
    let conn = db.connect_limbo();

    let ret = limbo_exec_rows(
        &db,
        &conn,
        "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT) WITHOUT ROWID;",
    );
    assert!(ret.is_empty());

    let sqlite_conn = rusqlite::Connection::open(db.path.clone())?;
    let sql: String =
        sqlite_conn.query_row("SELECT sql FROM sqlite_master WHERE name='t'", [], |row| {
            row.get(0)
        })?;
    assert!(sql.to_uppercase().contains("WITHOUT ROWID"));

    Ok(())
}

#[test]
fn test_select_from_without_rowid() -> anyhow::Result<()> {
    let db = TempDatabase::new_with_rusqlite(
        "CREATE TABLE t(a INTEGER PRIMARY KEY, b INTEGER) WITHOUT ROWID;",
    );
    {
        let sqlite_conn = rusqlite::Connection::open(db.path.clone())?;
        sqlite_conn.execute("INSERT INTO t VALUES (1,10)", params![])?;
        sqlite_conn.execute("INSERT INTO t VALUES (2,20)", params![])?;
    }
    let limbo_conn = db.connect_limbo();
    let query = "SELECT a, b FROM t ORDER BY a";
    let limbo_rows = limbo_exec_rows(&db, &limbo_conn, query);
    let sqlite_conn = rusqlite::Connection::open(db.path.clone())?;
    let sqlite_rows = sqlite_exec_rows(&sqlite_conn, query);
    assert_eq!(limbo_rows, sqlite_rows);
    Ok(())
}

#[test]
fn test_insert_into_without_rowid() -> anyhow::Result<()> {
    let db = TempDatabase::new_with_rusqlite(
        "CREATE TABLE t(a INTEGER PRIMARY KEY, b INTEGER) WITHOUT ROWID;",
    );
    let conn = db.connect_limbo();
    let ret = limbo_exec_rows(&db, &conn, "INSERT INTO t VALUES (1,1)");
    assert!(ret.is_empty());

    let query = "SELECT a, b FROM t ORDER BY a";
    let limbo_rows = limbo_exec_rows(&db, &conn, query);
    let sqlite_conn = rusqlite::Connection::open(db.path.clone())?;
    let sqlite_rows = sqlite_exec_rows(&sqlite_conn, query);
    assert_eq!(limbo_rows, sqlite_rows);
    Ok(())
}

#[test]
fn test_insert_duplicate_pk_without_rowid() -> anyhow::Result<()> {
    let db = TempDatabase::new_with_rusqlite(
        "CREATE TABLE t(a INTEGER PRIMARY KEY, b INTEGER) WITHOUT ROWID;",
    );
    let conn = db.connect_limbo();
    let ret = limbo_exec_rows(&db, &conn, "INSERT INTO t VALUES (1, 10)");
    assert!(ret.is_empty());
    let err = limbo_exec_rows_error(&db, &conn, "INSERT INTO t VALUES (1, 11)").unwrap_err();
    assert!(format!("{err:?}").contains("UNIQUE constraint failed"));
    Ok(())
}
#[test]
fn test_create_regular_table_has_rowid() -> anyhow::Result<()> {
    let db = TempDatabase::new_empty();
    let conn = db.connect_limbo();

    let ret = limbo_exec_rows(&db, &conn, "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT);");
    assert!(ret.is_empty());

    let sqlite_conn = rusqlite::Connection::open(db.path.clone())?;
    let sql: String =
        sqlite_conn.query_row("SELECT sql FROM sqlite_master WHERE name='t'", [], |row| {
            row.get(0)
        })?;
    assert!(!sql.to_uppercase().contains("WITHOUT ROWID"));

    Ok(())
}
