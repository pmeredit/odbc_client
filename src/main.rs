extern crate odbc;
// Use this crate and set environmet variable RUST_LOG=odbc to see ODBC warnings
extern crate env_logger;
use odbc::*;
use odbc_safe::AutocommitOn;
use std::io;
use clap::Clap;

#[derive(Clap)]
#[clap(version = "0.0.1", author = "Patrick Meredith <pmeredit@gmail.com>")]
struct Opts {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(Clap)]
enum SubCommand {
    #[clap(version = "0.0.1", autho = "Patrick Meredith <pmeredit@gmail.com>")]
    ListDSN(ListDSN),
    ListDrivers(ListDrivers),
    ReadTables(ReadTables),
    RunQuery(RunQuery),
}

#[derive(Clap)]
struct ListDSN {}

#[derive(Clap)]
struct ListDrivers {}

#[derive(Clap)]
struct ReadTables {
    #[clap(short, long, default_value = "%")]
    connection_string: String,
    #[clap(short, long, default_value = "%")]
    catalog_name: String,
    #[clap(short, long, default_value = "%")]
    table_name: String,
}

#[derive(Clap)]
struct RunQuery {
    #[clap(short, long)]
    connection_string: String,
    #[clap(short, long)]
    catalog_name: String,
    #[clap(short, long)]
    table_name: String,
}

fn main() {
    env_logger::init();

    let opts: Opts = Opts::parse();

    match connect(&opts) {
        Ok(()) => println!("Success"),
        Err(diag) => println!("Error: {}", diag),
    }
}

fn connect(opts: &Opts) -> std::result::Result<(), DiagnosticRecord> {
    let mut env = create_environment_v3().map_err(|e| e.unwrap())?;

    match opts.subcommand {
        ListDSN(_) => {
            for ds in env.system_data_sources()? {
                println!("{:?}", ds);
            }
        }
        ListDrivers(_) => {
            for driver in env.drivers()? {
                println!("{:?}", driver);
            }
        }
        ReadTables(r) => {
            let conn = env.connect_with_connection_string(&r.connection_string)?;
            execute_tables(&conn, r.query)?;
        }
        RunQuery(r) => {
            let conn = env.connect_with_connection_string(&r.connection_string)?;
            execute_statement(&conn, r.catalog_name, r.table_name)?;
        }
    }
}

fn execute_statement<'env>(conn: &Connection<'env, AutocommitOn>, query: &str) -> Result<()> {
    let stmt = Statement::with_parent(conn)?;

    match stmt.exec_direct(query)? {
        Data(mut stmt) => {
            let cols = stmt.num_result_cols()?;
            while let Some(mut cursor) = stmt.fetch()? {
                println!("==  Row as Strings");
                for i in 1..(cols + 1) {
                    match cursor.get_data::<&str>(i as u16)? {
                        Some(val) => print!(" | {}", val),
                        None => print!(" | NULL"),
                    }
                }
                println!("");
                println!("==  Row as f64");
                for i in 1..(cols + 1) {
                    match cursor.get_data::<f64>(i as u16)? {
                        Some(val) => print!(" | {}", val),
                        None => print!(" | NULL"),
                    }
                }
                println!("");
                println!("==  Row as f32");
                for i in 1..(cols + 1) {
                    match cursor.get_data::<f32>(i as u16)? {
                        Some(val) => print!(" | {}", val),
                        None => print!(" | NULL"),
                    }
                }
                println!("");
                println!("==  Row as i32");
                for i in 1..(cols + 1) {
                    match cursor.get_data::<i32>(i as u16)? {
                        Some(val) => print!(" | {}", val),
                        None => print!(" | NULL"),
                    }
                }
                println!("");
                println!("==  Row as bool");
                for i in 1..(cols + 1) {
                    match cursor.get_data::<bool>(i as u16)? {
                        Some(val) => print!(" | {}", val),
                        None => print!(" | NULL"),
                    }
                }
                println!("");
            }
        }
        NoData(_) => println!("Query executed, no data returned"),
    }
    Ok(())
}

fn execute_tables<'env>(conn: &Connection<'env, AutocommitOn>, catalog_name: &str, table_name: &str) -> Result<()> {
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
}
