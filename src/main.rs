
use std::{io, fs::File, collections::HashMap};

use bigdecimal::{BigDecimal, Zero};
use serde::{Deserialize, Serialize};

const PRECISION: u64 = 5;

/// An enumeration of each transaction type.
#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TransactionType {
    /// A deposit is a credit to the client's account.
    Deposit,
    
    /// A withdraw is a debit to the client's account.
    /// This should fail if the withdrawal amount is greater than the available balance.
    Withdrawal,

    /// A dispute represents a client's claim that a transaction was erroneous and should be reversed.
    Dispute,

    /// A resolve represents a resolution to a dispute, releasing the associated held funds.
    /// Funds that were previously disputed are no longer disputed.
    Resolve,

    /// A chargeback is the final state of a dispute and represents the client reversing a transaction.
    /// Funds that were held have now been withdrawn. Lock the account after this.
    Chargeback,
}

/// A structure to represent a transaction.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Transaction {
    /// The transaction type.
    #[serde(rename = "type")]
    type_: TransactionType,

    /// The id of the client.
    #[serde(rename = "client")]
    client_id: u16,

    /// The id of the transaction.
    #[serde(rename = "tx")]
    id: u32,

    /// The amount associated with the transaction.
    #[serde(default)]
    amount: Option<BigDecimal>,

    /// Whether the transaction is in dispute.
    #[serde(skip)]
    disputed: bool
}

/// A structure to represent a specific client's account.
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct Client {
    /// The id associated with the client.
    id: u16,
    
    /// The total funds that are available for trading, staking, withdrawal, etc.
    available: BigDecimal,

    /// The total funds that are held for dispute.
    held: BigDecimal,

    /// The total funds that are available or held.
    total: BigDecimal,

    /// Whether the account is locked.
    locked: bool,

    #[serde(skip)]
    /// The transactions associated with the client.
    // NOTE: This wouldn't be used in a real system, but is used here to keep things simple.
    transactions: HashMap<u32, Transaction>
}

impl Client {
    fn new(id: u16) -> Self {
        Self {
            id,
            available: BigDecimal::zero().with_prec(PRECISION),
            held: BigDecimal::zero().with_prec(PRECISION),
            total: BigDecimal::zero().with_prec(PRECISION),
            ..Default::default()
        }
    }

    fn add_transaction(&mut self, transaction: &Transaction) {
        assert!(transaction.amount.is_some());

        let transaction = Transaction {
            type_: transaction.type_,
            client_id: self.id,
            id: transaction.id,
            // BigDecimal is not Copy, so we need to clone the amount. Also, force precision.
            amount: Some(transaction.amount.as_ref().unwrap().clone().with_prec(PRECISION)),
            disputed: transaction.disputed
        };

        self.transactions.insert(transaction.id, transaction);
    }

    pub fn process_transaction(&mut self, transaction: &Transaction) {
        if self.locked {
            // NOTE: This wasn't specified, but I made the assumption that a locked account should not have any transactions processed.
            return;
        }

        match transaction.type_ {
            TransactionType::Deposit => {
                if let Some(amount) = &transaction.amount.as_ref().map(|amount| amount.with_prec(PRECISION)) {
                    self.available += amount;
                    self.total += amount;

                    self.add_transaction(transaction);
                }
            },
            TransactionType::Withdrawal => {
                if let Some(amount) = &transaction.amount.as_ref().map(|amount| amount.with_prec(PRECISION)) {
                    if amount <= &self.available {
                        self.available -= amount;
                        self.total -= amount;

                        self.add_transaction(transaction);
                    }
                }
            },
            TransactionType::Dispute | TransactionType::Resolve | TransactionType::Chargeback => {
                if let Some(target) = self.transactions.get_mut(&transaction.id) {
                    match transaction.type_ {
                        TransactionType::Dispute if !target.disputed => {
                            let amount = target.amount.as_ref().unwrap();

                            self.available -= amount;
                            self.held += amount;

                            target.disputed = true;
                        },
                        TransactionType::Resolve if target.disputed => {
                            let amount = target.amount.as_ref().unwrap();
                            
                            self.held -= amount;
                            self.available += amount;
                                
                            target.disputed = false;
                        },
                        TransactionType::Chargeback if target.disputed => {
                            let amount = target.amount.as_ref().unwrap();

                            self.held -= amount;
                            self.total -= amount;

                            self.locked = true;
                        },
                        _ => {}
                    }
                }
            }
        }
    }
}

fn transactions_from_reader<R: io::Read>(reader: R) -> io::Result<Vec<Transaction>> {
    Ok(csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_reader(reader)
        .deserialize::<Transaction>()
        .collect::<Result<Vec<_>, _>>()?)
}

fn main() {
    let args = std::env::args().collect::<Vec<_>>();

    if args.len() != 2 {
        println!("Usage: {} <input_file>", args[0]);
        std::process::exit(1);
    }

    let clients = File::open(&args[1])
        .map(io::BufReader::new)
        .and_then(transactions_from_reader)
        .map(|transactions| {
            let mut clients = HashMap::new();
            for transaction in transactions {
                clients.entry(transaction.client_id)
                    .or_insert_with(|| Client::new(transaction.client_id))
                    .process_transaction(&transaction);
            }
            clients
        });
    
    match clients {
        Ok(clients) => {
            let mut writer = csv::Writer::from_writer(std::io::stdout());
            for client in clients.values() {
                if writer.serialize(client).is_err() {
                    println!("Error: unable to serialize client {} to csv", client.id);
                    std::process::exit(1);
                }
            }
        },
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            println!("Error: input file '{}' does not exist", args[1]);
            std::process::exit(1);
        },
        Err(e) if e.kind() == io::ErrorKind::PermissionDenied => {
            println!("Error: input file '{}' is not readable", args[1]);
            std::process::exit(1);
        },
        Err(_) => {
            // TODO: Log the error to stderr, so we can verify that this case is only DeserializerError.
            println!("Error: input file '{}' has an invalid format", args[1]);
            std::process::exit(1);
        },
    }
}


#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use bigdecimal::Zero;

    use super::*;

    #[test]
    fn simple_deposit() {
        let amount = BigDecimal::from_str("100").unwrap();

        let mut client = Client::new(1);

        client.process_transaction(&Transaction {
            type_: TransactionType::Deposit,
            client_id: 1,
            id: 1,
            amount: Some(amount.clone()),
            disputed: Default::default()
        });

        assert_eq!(client.available, amount);
        assert_eq!(client.total, amount);
    }

    #[test]
    fn simple_withdrawal() {
        let amount = BigDecimal::from_str("50").unwrap();

        let mut client = Client::new(1);

        client.process_transaction(&Transaction {
            type_: TransactionType::Deposit,
            client_id: 1,
            id: 1,
            amount: Some(BigDecimal::from_str("100").unwrap()),
            disputed: Default::default()
        });

        client.process_transaction(&Transaction {
            type_: TransactionType::Withdrawal,
            client_id: 1,
            id: 2,
            amount: Some(amount.clone()),
            disputed: Default::default()
        });

        assert_eq!(client.available, amount);
        assert_eq!(client.total, amount);
    }

    #[test]
    fn simple_dispute() {
        let amount = BigDecimal::from_str("100").unwrap();

        let mut client = Client::new(1);

        client.process_transaction(&Transaction {
            type_: TransactionType::Deposit,
            client_id: 1,
            id: 1,
            amount: Some(amount.clone()),
            disputed: Default::default()
        });

        client.process_transaction(&Transaction {
            type_: TransactionType::Dispute,
            client_id: 1,
            id: 1,
            amount: Default::default(),
            disputed: Default::default()
        });

        assert_eq!(client.available, BigDecimal::zero().with_prec(PRECISION));
        assert_eq!(client.held, amount);
        assert_eq!(client.total, amount);
        assert!(client.transactions.get(&1).unwrap().disputed);
    }

    #[test]
    fn simple_dispute_to_resolve() {
        let amount = BigDecimal::from_str("100").unwrap();

        let mut client = Client::new(1);

        client.process_transaction(&Transaction {
            type_: TransactionType::Deposit,
            client_id: 1,
            id: 1,
            amount: Some(amount.clone()),
            disputed: Default::default()
        });

        client.process_transaction(&Transaction {
            type_: TransactionType::Dispute,
            client_id: 1,
            id: 1,
            amount: Default::default(),
            disputed: Default::default()
        });

        client.process_transaction(&Transaction {
            type_: TransactionType::Resolve,
            client_id: 1,
            id: 1,
            amount: Default::default(),
            disputed: Default::default()
        });

        assert_eq!(client.available, amount);
        assert_eq!(client.held, BigDecimal::zero().with_prec(PRECISION));
        assert_eq!(client.total, amount);
        assert!(!client.transactions.get(&1).unwrap().disputed);
    }

    #[test]
    fn simple_dispute_to_chargeback() {
        let amount = BigDecimal::from_str("100").unwrap();

        let mut client = Client::new(1);

        client.process_transaction(&Transaction {
            type_: TransactionType::Deposit,
            client_id: 1,
            id: 1,
            amount: Some(amount),
            disputed: Default::default()
        });

        client.process_transaction(&Transaction {
            type_: TransactionType::Dispute,
            client_id: 1,
            id: 1,
            amount: Default::default(),
            disputed: Default::default()
        });

        client.process_transaction(&Transaction {
            type_: TransactionType::Chargeback,
            client_id: 1,
            id: 1,
            amount: Default::default(),
            disputed: Default::default()
        });

        assert!(client.locked);
    }

    #[test]
    fn csv_example() {
        let csv = "type,       client,     tx,     amount
                        deposit,    1,          1,      1.0001
                        deposit,    2,          2,      2.0001
                        deposit,    1,          3,      2.0001
                        withdrawal, 1,          4,      1.5002
                        withdrawal, 2,          5,      3.0001
                        dispute,    2,          5,
                        resolve,    2,          5,
                        dispute,    1,          3,
                        chargeback, 1,          3,";

        let transactions = transactions_from_reader(io::BufReader::new(csv.as_bytes())).unwrap();
        
        let mut clients: HashMap<u16, Client> = HashMap::new();
        for transaction in transactions {
            clients.entry(transaction.client_id)
                .or_insert_with(|| Client::new(transaction.client_id))
                .process_transaction(&transaction);
        }

          
        assert_eq!(clients.len(), 2);
        assert!(clients.get(&1).unwrap().locked);

        assert_eq!(clients.get(&2).unwrap().available, BigDecimal::from_str("2.0001").unwrap().with_prec(PRECISION));
        assert!(!clients.get(&2).unwrap().locked);
    }
}