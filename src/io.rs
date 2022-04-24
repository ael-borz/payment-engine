use std::{io::{Read, BufWriter, Write}, collections::HashMap, error::Error};

use crate::engine::{ClientState, TransactionSummary, handle_transaction, Transaction, ClientId, TransactionId};


/// Reads a source formated as a CSV and deserialize its content.
/// Each line from the source should represent a transaction.
/// 
/// # Arguments
/// 
/// `from` - source that should implement the Read trait
pub fn csv_reader(from: impl Read) -> Result<HashMap<ClientId, ClientState>, Box<dyn Error>> {
    let mut transaction_history: HashMap<(ClientId, TransactionId), TransactionSummary> = HashMap::new();
    let mut clients_state: HashMap<ClientId, ClientState> = HashMap::new();

    let mut reader = csv::ReaderBuilder::new()
        .trim(csv::Trim::All) // In order to handle whitespaces
        .from_reader(from);

    for result in reader.deserialize() {
        let transaction: Transaction = result?;
        handle_transaction(&transaction, &mut transaction_history, &mut clients_state);
    }
    Ok(clients_state)
}

/// Writes to source formated as a CSV.
/// Each line written represents a client's final state.
/// 
/// # Arguments
/// 
/// `to` - destination that should implement the Write trait
pub fn csv_writer(clients_state: HashMap<ClientId, ClientState>, to: impl Write) -> Result<(), std::io::Error> {
    let mut stream = BufWriter::new(to);
    stream.write(b"client,available,held,total,locked")?;
    for (client_id, client_state) in clients_state {
        write!(
            stream,
            "\n{},{:.4},{:.4},{:.4},{}",
            client_id,
            client_state.available,
            client_state.held,
            client_state.total,
            client_state.locked
        )?;
    }
    Ok(())
}


#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn deposits_increase_total_and_available_funds() {
        let input = "type,client,tx,amount\ndeposit,1,1,1.0\ndeposit,1,2,1.0".as_bytes();

        let mut expected_clients_state: HashMap<ClientId, ClientState> = HashMap::new();
        expected_clients_state.insert(
            1,
            ClientState {
                available: 2.0,
                held: 0.0,
                total: 2.0,
                locked: false,
            },
        );

        let clients_state = csv_reader(input).unwrap();

        assert_eq!(clients_state, expected_clients_state);
    }

    #[test]
    fn withdrawal_with_funds_success() {
        let input = "type,client,tx,amount\ndeposit,1,1,1.5\nwithdrawal,1,2,1.5".as_bytes();

        let mut expected_clients_state: HashMap<ClientId, ClientState> = HashMap::new();
        expected_clients_state.insert(
            1,
            ClientState {
                available: 0.0,
                held: 0.0,
                total: 0.0,
                locked: false,
            },
        );

        let clients_state = csv_reader(input).unwrap();

        assert_eq!(clients_state, expected_clients_state);
    }

    #[test]
    fn withdrawal_without_funds_is_ignored() {
        let input = "type,client,tx,amount\ndeposit,1,1,1.0\nwithdrawal,1,2,1.5".as_bytes();

        let mut expected_clients_state: HashMap<ClientId, ClientState> = HashMap::new();
        expected_clients_state.insert(
            1,
            ClientState {
                available: 1.0,
                held: 0.0,
                total: 1.0,
                locked: false,
            },
        );

        let clients_state = csv_reader(input).unwrap();

        assert_eq!(clients_state, expected_clients_state);
    }

    #[test]
    fn dispute_increases_held_and_decreases_available() {
        let input = "type,client,tx,amount\ndeposit,1,1,1.0\ndispute,1,1,".as_bytes();

        let mut expected_clients_state: HashMap<ClientId, ClientState> = HashMap::new();
        expected_clients_state.insert(
            1,
            ClientState {
                available: 0.0,
                held: 1.0,
                total: 1.0,
                locked: false,
            },
        );

        let clients_state = csv_reader(input).unwrap();

        assert_eq!(clients_state, expected_clients_state);
    }

    #[test]
    fn dispute_on_inexistant_transaction_is_ignored() {
        let input = "type,client,tx,amount\ndeposit,1,1,1.0\ndispute,1,0,".as_bytes();

        let mut expected_clients_state: HashMap<ClientId, ClientState> = HashMap::new();
        expected_clients_state.insert(
            1,
            ClientState {
                available: 1.0,
                held: 0.0,
                total: 1.0,
                locked: false,
            },
        );

        let clients_state = csv_reader(input).unwrap();

        assert_eq!(clients_state, expected_clients_state);
    }

    #[test]
    fn dispute_on_other_client_transaction_is_ignored() {
        let input =
            "type,client,tx,amount\ndeposit,1,1,1.0\ndeposit,2,2,1.0\ndispute,1,2,".as_bytes();

        let mut expected_clients_state: HashMap<ClientId, ClientState> = HashMap::new();
        expected_clients_state.insert(
            1,
            ClientState {
                available: 1.0,
                held: 0.0,
                total: 1.0,
                locked: false,
            },
        );
        expected_clients_state.insert(
            2,
            ClientState {
                available: 1.0,
                held: 0.0,
                total: 1.0,
                locked: false,
            },
        );

        let clients_state = csv_reader(input).unwrap();

        assert_eq!(clients_state, expected_clients_state);
    }

    #[test]
    fn resolve_releases_disputed_transaction_held_funds() {
        let input = "type,client,tx,amount\ndeposit,1,1,1.0\ndispute,1,1,\nresolve,1,1,".as_bytes();

        let mut expected_clients_state: HashMap<ClientId, ClientState> = HashMap::new();
        expected_clients_state.insert(
            1,
            ClientState {
                available: 1.0,
                held: 0.0,
                total: 1.0,
                locked: false,
            },
        );

        let clients_state = csv_reader(input).unwrap();

        assert_eq!(clients_state, expected_clients_state);
    }

    #[test]
    fn resolve_on_non_disputed_transaction_is_ignored() {
        let input = "type,client,tx,amount\ndeposit,1,1,1.0\nresolve,1,1,".as_bytes();

        let mut expected_clients_state: HashMap<ClientId, ClientState> = HashMap::new();
        expected_clients_state.insert(
            1,
            ClientState {
                available: 1.0,
                held: 0.0,
                total: 1.0,
                locked: false,
            },
        );

        let clients_state = csv_reader(input).unwrap();

        assert_eq!(clients_state, expected_clients_state);
    }

    #[test]
    fn resolve_on_inexistant_transaction_is_ignored() {
        let input = "type,client,tx,amount\ndeposit,1,1,1.0\nresolve,1,10,".as_bytes();

        let mut expected_clients_state: HashMap<ClientId, ClientState> = HashMap::new();
        expected_clients_state.insert(
            1,
            ClientState {
                available: 1.0,
                held: 0.0,
                total: 1.0,
                locked: false,
            },
        );

        let clients_state = csv_reader(input).unwrap();

        assert_eq!(clients_state, expected_clients_state);
    }

    #[test]
    fn resolve_on_already_resolved_transaction_is_ignored() {
        let input =
            "type,client,tx,amount\ndeposit,1,1,1.0\ndispute,1,1,\nresolve,1,1,\nresolve,1,1,"
                .as_bytes();

        let mut expected_clients_state: HashMap<ClientId, ClientState> = HashMap::new();
        expected_clients_state.insert(
            1,
            ClientState {
                available: 1.0,
                held: 0.0,
                total: 1.0,
                locked: false,
            },
        );

        let clients_state = csv_reader(input).unwrap();

        assert_eq!(clients_state, expected_clients_state);
    }

    #[test]
    fn chargeback_locks_client_and_decreases_held_and_total_funds() {
        let input =
            "type,client,tx,amount\ndeposit,1,1,1.0\ndispute,1,1,\nchargeback,1,1,".as_bytes();

        let mut expected_clients_state: HashMap<ClientId, ClientState> = HashMap::new();
        expected_clients_state.insert(
            1,
            ClientState {
                available: 0.0,
                held: 0.0,
                total: 0.0,
                locked: true,
            },
        );

        let clients_state = csv_reader(input).unwrap();

        assert_eq!(clients_state, expected_clients_state);
    }

    #[test]
    fn chargeback_on_already_chargedback_is_ignored() {
        let input = "type,client,tx,amount\ndeposit,1,1,1.0\ndispute,1,1,\nchargeback,1,1,\nchargeback,1,1,".as_bytes();

        let mut expected_clients_state: HashMap<ClientId, ClientState> = HashMap::new();
        expected_clients_state.insert(
            1,
            ClientState {
                available: 0.0,
                held: 0.0,
                total: 0.0,
                locked: true,
            },
        );

        let clients_state = csv_reader(input).unwrap();

        assert_eq!(clients_state, expected_clients_state);
    }

    #[test]
    fn chargeback_on_inexistant_transaction_is_ignored() {
        let input = "type,client,tx,amount\ndeposit,1,1,1.0\ndispute,1,1,\nchargeback,1,10,".as_bytes();

        let mut expected_clients_state: HashMap<ClientId, ClientState> = HashMap::new();
        expected_clients_state.insert(
            1,
            ClientState {
                available: 0.0,
                held: 1.0,
                total: 1.0,
                locked: false,
            },
        );

        let clients_state = csv_reader(input).unwrap();

        assert_eq!(clients_state, expected_clients_state);
    }

    #[test]
    fn chargeback_on_non_disputed_transaction_is_ignored() {
        let input = "type,client,tx,amount\ndeposit,1,1,1.0\nchargeback,1,1,".as_bytes();

        let mut expected_clients_state: HashMap<ClientId, ClientState> = HashMap::new();
        expected_clients_state.insert(
            1,
            ClientState {
                available: 1.0,
                held: 0.0,
                total: 1.0,
                locked: false,
            },
        );

        let clients_state = csv_reader(input).unwrap();

        assert_eq!(clients_state, expected_clients_state);
    }

    #[test]
    fn chargeback_prevents_any_other_transactions() {
        let input = 
        "type,client,tx,amount\ndeposit,1,1,1.0\ndispute,1,1,\nchargeback,1,1,\ndeposit,1,2,1.0\nwithdrawal,1,3,0.5\ndispute,1,3,\nresolve,1,3,\n"
        .as_bytes();

        let mut expected_clients_state: HashMap<ClientId, ClientState> = HashMap::new();
        expected_clients_state.insert(
            1,
            ClientState {
                available: 0.0,
                held: 0.0,
                total: 0.0,
                locked: true,
            },
        );

        let clients_state = csv_reader(input).unwrap();

        assert_eq!(clients_state, expected_clients_state);
    }

    #[test]
    fn output_is_correctly_formated() {
        let input = "type,client,tx,amount
deposit,1,1,1.0
deposit,2,2,2.0
deposit,1,3,2.0
withdrawal,1,4,1.5
withdrawal,2,5,3.0"
            .as_bytes();

        let expected_lines = [
            "client,available,held,total,locked",
            "1,1.5000,0.0000,1.5000,false",
            "2,2.0000,0.0000,2.0000,false"
        ];

        let clients_state = csv_reader(input).unwrap();

        let mut utf8_output = Vec::new();
        csv_writer(clients_state, &mut utf8_output).unwrap();

        let str_output = String::from_utf8(utf8_output).unwrap();
        
        for expected_line in expected_lines {
            assert!(str_output.contains(expected_line));
        }
    }
}