use std::collections::HashMap;

use serde::Deserialize;

pub type TransactionId = u32;
pub type ClientId = u16;

/// Represents a transaction done by a client.
#[derive(Deserialize, Debug, PartialEq)]
pub struct Transaction {
    /// Type of transaction, one of (deposit, withdrawal, dispute, resolve, chargeback)
    #[serde(rename(deserialize = "type"))]
    tx_type: String,
    client: ClientId,
    tx: TransactionId,
    /// Can be None if tx_type is dispute, resolve or chargeback
    amount: Option<f64>,
}

impl Transaction {
    fn get_amount(&self) -> f64 {
        self.amount.unwrap_or_default()
    }
}

/// Represents the final state of a client after handling all of his transaction_history.
#[derive(Deserialize, Debug, PartialEq, Default)]
pub struct ClientState {
    pub available: f64,
    pub held: f64,
    pub total: f64,
    pub locked: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct TransactionSummary {
    pub amount: f64,
    pub is_disputed: bool,
}

/// Handles deposit transaction by updating client's state and adding current transaction to history.
/// 
/// Increases available and total.
fn handle_deposit(
    transaction: &Transaction,
    transaction_history: &mut HashMap<(ClientId, TransactionId), TransactionSummary>,
    clients_state: &mut HashMap<ClientId, ClientState>
) {
    // We historize the transaction in order to deal with disputes, resolves, and chargebacks later.
    transaction_history.insert(
        (transaction.client, transaction.tx),
        TransactionSummary {
            amount: transaction.get_amount(),
            is_disputed: false,
        },
    );
    clients_state
        .entry(transaction.client)
        .and_modify(|client_state| {
            if !client_state.locked {
                client_state.available += transaction.get_amount();
                client_state.total += transaction.get_amount();
            }
        })
        .or_insert(ClientState {
            available: transaction.get_amount(),
            held: 0.0,
            total: transaction.get_amount(),
            locked: false,
        });
}

/// Handles withdrawal transaction by updating client's state and adding current transaction to history.
/// 
/// Decreases available and total.
fn handle_withdrawal(
    transaction: &Transaction,
    transaction_history: &mut HashMap<(ClientId, TransactionId), TransactionSummary>,
    clients_state: &mut HashMap<ClientId, ClientState>
) {
    // We historize the transaction in order to deal with disputes, resolves, and chargebacks later.
    transaction_history.insert(
        (transaction.client, transaction.tx),
        TransactionSummary {
            amount: transaction.get_amount(),
            is_disputed: false,
        },
    );
    clients_state
        .entry(transaction.client)
        .and_modify(|client_state| {
            if !client_state.locked
                && client_state.available >= transaction.get_amount()
            {
                client_state.available -= transaction.get_amount();
                client_state.total -= transaction.get_amount();
            }
        })
        .or_default(); // Create a new record
}

/// Handles dispute transaction by updating client's state.
/// 
/// Decreases available, increases held and flags transaction as disputed.
fn handle_dispute(
    transaction: &Transaction,
    transaction_history: &mut HashMap<(ClientId, TransactionId), TransactionSummary>,
    clients_state: &mut HashMap<ClientId, ClientState>
) {
    clients_state
        .entry(transaction.client)
        .and_modify(|client_state| {
            // By design, we ensure that the referenced transaction belongs to the client
            // which prevents a client from disputing another client's transaction.
            if let Some(referenced_transaction) =
                transaction_history.get_mut(&(transaction.client, transaction.tx))
            {
                if !client_state.locked {
                    client_state.available -= referenced_transaction.amount;
                    client_state.held += referenced_transaction.amount;
                    referenced_transaction.is_disputed = true;
                }
            }
        })
        .or_default();
}

/// Handles withdrawal transaction by updating client's state
/// 
/// Decreases held, increases available and flags transaction as no longer disputed.
fn handle_resolve(
    transaction: &Transaction,
    transaction_history: &mut HashMap<(ClientId, TransactionId), TransactionSummary>,
    clients_state: &mut HashMap<ClientId, ClientState>
) {
    clients_state
        .entry(transaction.client)
        .and_modify(|client_state| {
            // By design, we ensure that the referenced transaction belongs to the client
            // which prevents a client from disputing another client's transaction.
            if let Some(referenced_transaction) =
                transaction_history.get_mut(&(transaction.client, transaction.tx))
            {
                if !client_state.locked && referenced_transaction.is_disputed {
                    client_state.held -= referenced_transaction.amount;
                    client_state.available += referenced_transaction.amount;
                    referenced_transaction.is_disputed = false;
                }
            }
        })
        .or_default();
}

/// Handles withdrawal transaction by updating client's state
/// 
/// Decreases held and total, and flags transaction as no longer disputed.
/// 
/// Also flags the client's state as locked.
fn handle_chargeback(
    transaction: &Transaction,
    transaction_history: &mut HashMap<(ClientId, TransactionId), TransactionSummary>,
    clients_state: &mut HashMap<ClientId, ClientState>
) { 
    clients_state
        .entry(transaction.client)
        .and_modify(|client_state| {
            // By design, we ensure that the referenced transaction belongs to the client
            // which prevents a client from disputing another client's transaction.
            if let Some(referenced_transaction) =
                transaction_history.get_mut(&(transaction.client, transaction.tx))
            {
                if !client_state.locked && referenced_transaction.is_disputed {
                    client_state.held -= referenced_transaction.amount;
                    client_state.total -= referenced_transaction.amount;
                    referenced_transaction.is_disputed = false;
                    client_state.locked = true;
                }
            }
        })
        .or_default();
}

/// Dispatches receiving transaction to the correct handler.
/// 
/// Transaction type must be one of "deposit", "withdrawal", "dispute", "resolve", or "chargeback"
/// 
/// There will be no update if the client's account is locked.
/// 
/// # Arguments
/// 
/// * `transaction` - the current transaction
/// * `transaction_history` - history of all previous transaction_history, identified by client id and transaction id respectively
/// * `clients_state` - the current state of all clients, identified by client id
pub fn handle_transaction(
    transaction: &Transaction,
    transaction_history: &mut HashMap<(ClientId, TransactionId), TransactionSummary>,
    clients_state: &mut HashMap<ClientId, ClientState>
) {
    match transaction.tx_type.as_str() {
        "deposit" => handle_deposit(transaction, transaction_history, clients_state),
        "withdrawal" => handle_withdrawal(transaction, transaction_history, clients_state),
        "dispute" => handle_dispute(transaction, transaction_history, clients_state),
        "resolve" => handle_resolve(transaction, transaction_history, clients_state),
        "chargeback" => handle_chargeback(transaction, transaction_history, clients_state),
        _ => eprintln!("Error: unrecognized transaction type {}", transaction.tx_type)
    }
}

