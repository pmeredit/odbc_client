extern crate odbc;
// Use this crate and set environmet variable RUST_LOG=odbc to see ODBC warnings
extern crate env_logger;
use odbc::*;
use odbc_safe::AutocommitOn;
use clap::Clap;

#[derive(Clap)]
#[clap(version = "0.0.1", author = "Patrick Meredith <pmeredit@gmail.com>")]
struct Opts {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(Clap)]
enum SubCommand {
    ListDSN,
    ListDrivers,
    ListTables(ListTables),
    RunQuery(RunQuery),
}

#[derive(Clap)]
struct ListTables {
    #[clap(short, long)]
    uri: String,
    #[clap(short, long, default_value = "%")]
    catalog_name: String,
    #[clap(short, long, default_value = "%")]
    table_name: String,
}

#[derive(Clap)]
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
        Err(diag) => println!("Error: {}", diag),
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
    Ok(())
}
