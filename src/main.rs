extern crate odbc;
// Use this crate and set environmet variable RUST_LOG=odbc to see ODBC warnings
extern crate env_logger;
use clap::Parser;
use odbc::*;
use odbc_safe::AutocommitOn;

#[derive(Parser)]
#[clap(version = "0.0.1", author = "Patrick Meredith <pmeredit@gmail.com>")]
struct Opts {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(Parser)]
enum SubCommand {
    ListDSN,
    ListDrivers,
    ListTables(ListTables),
    RunQuery(RunQuery),
}

#[derive(Parser)]
struct ListTables {
    #[clap(short, long)]
    uri: String,
    #[clap(short, long, default_value = "%")]
    catalog_name: String,
    #[clap(short, long, default_value = "%")]
    table_name: String,
}

#[derive(Parser)]
struct RunQuery {
    #[clap(short, long)]
    uri: String,
    #[clap(short, long)]
    query: String,
}

fn main() {
    env_logger::init();

    let opts: Opts = Opts::parse();

    match connect(&opts) {
        Ok(()) => println!("Success"),
        Err(diag) => println!("Error Connection: {}", diag),
    }
}

fn connect(opts: &Opts) -> std::result::Result<(), DiagnosticRecord> {
    let mut env = create_environment_v3().map_err(|e| e.unwrap())?;

    match &opts.subcmd {
        SubCommand::ListDSN => {
            for ds in env.system_data_sources()? {
                println!("{:?}", ds);
            }
            Ok(())
        }
        SubCommand::ListDrivers => {
            for driver in env.drivers()? {
                println!("{:?}", driver);
            }
            Ok(())
        }
        SubCommand::ListTables(r) => {
            let conn = env.connect_with_connection_string(&r.uri)?;
            execute_tables(&conn, &r.catalog_name, &r.table_name)
        }
        SubCommand::RunQuery(r) => {
            println!("{:?}", r.uri);
            let conn = env.connect_with_connection_string(&r.uri)?;
            execute_statement(&conn, &r.query)
        }
    }
}

fn execute_statement<'env>(conn: &Connection<'env, AutocommitOn>, query: &str) -> Result<()> {
    let stmt = Statement::with_parent(conn)?;

    match stmt.exec_direct(query)? {
        Data(mut stmt) => {
            let cols = stmt.num_result_cols()?;
            //println!("== Description");
            let mut descriptions = Vec::with_capacity(cols as usize);
            for i in 1..(cols + 1) {
                let desc = stmt
                    .describe_col(i as u16)
                    .expect("failed to get description");
                println!("{:?}", desc);
                descriptions.push(desc);
            }
            while let Some(mut cursor) = stmt.fetch()? {
                for i in 1..(cols + 1) {
                    match descriptions[(i - 1) as usize].data_type {
                        ffi::SqlDataType::SQL_EXT_BIT => {
                            match cursor.get_data::<bool>(i as u16)? {
                                Some(val) => print!(" | {}", val),
                                None => print!(" | NULL"),
                            }
                        }
                        ffi::SqlDataType::SQL_CHAR => match cursor.get_data::<&str>(i as u16)? {
                            Some(val) => print!(" | {}", val),
                            None => print!(" | NULL"),
                        },
                        ffi::SqlDataType::SQL_DATE | ffi::SqlDataType::SQL_DATETIME => {
                            match cursor.get_data::<ffi::SQL_TIMESTAMP_STRUCT>(i as u16)? {
                                Some(val) => print!(" | {:?}", val),
                                None => print!(" | NULL"),
                            }
                        }
                        ffi::SqlDataType::SQL_EXT_BIGINT => {
                            match cursor.get_data::<i64>(i as u16)? {
                                Some(val) => print!(" | {}", val),
                                None => print!(" | NULL"),
                            }
                        }
                        ffi::SqlDataType::SQL_DOUBLE => match cursor.get_data::<f64>(i as u16)? {
                            Some(val) => print!(" | {}", val),
                            None => print!(" | NULL"),
                        },
                        _ => {
                            print!("| UNKNOWN TYPE");
                        }
                    }
                }
                println!("");
            }
        }
        NoData(_) => println!("Query executed, no data returned"),
    }
    Ok(())
}

fn execute_tables<'env>(
    conn: &Connection<'env, AutocommitOn>,
    catalog_name: &str,
    table_name: &str,
) -> Result<()> {
    let stmt = Statement::with_parent(conn)?;
    let mut stmt = stmt.tables_str(catalog_name, "", table_name, "")?;
    let cols = stmt.num_result_cols()?;
    println!("TABLES");
    while let Some(mut cursor) = stmt.fetch()? {
        for i in 1..(cols + 1) {
            match cursor.get_data::<&str>(i as u16)? {
                Some(val) => print!(" | {}", val),
                None => print!(" | NULL"),
            }
        }
        println!("");
    }
    println!("");
    Ok(())
}
