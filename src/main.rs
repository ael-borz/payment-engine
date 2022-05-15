use std::fs::File;

use payment_engine::io::{csv_reader, csv_writer};

/// Entrypoint of the application, filepath expected
fn main() -> Result<(), std::io::Error> {
    let path = std::env::args()
        .nth(1)
        .expect("Error: missing filepath parameter");
    let csv_file = File::open(path)?;

    match csv_reader(&csv_file) {
        Err(err) => panic!("{err}"),
        Ok(clients_state) => {
            let stdout = std::io::stdout();
            let handle = stdout.lock(); // better performance on single threaded program
            csv_writer(clients_state, handle)?
        }
    }  

    Ok(())
}
