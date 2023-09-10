use super::*;
use crate::index::{ConstructTransaction, MysqlDatabase, TransactionOutputArray};
use bitcoin::blockdata::{script, witness::Witness};
use bitcoin::consensus::encode::serialize_hex;
use bitcoin::psbt::Psbt;
use bitcoin::{AddressType, PackedLockTime};

#[derive(Debug, Parser)]
pub struct Cancel {
  #[clap(long, help = "Send inscription from <SOURCE>.")]
  pub source: Address,
  #[clap(long, help = "Send inscription to <DESTINATION>.")]
  pub destination: Address,
  #[clap(long, help = "The inputs that needs to be canceled.")]
  pub inputs: Vec<OutPoint>,
  #[clap(long, help = "Use fee rate of <FEE_RATE> sats/vB")]
  pub fee_rate: FeeRate,
  pub default_amount: Option<Amount>
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Output {
  pub transaction: String,
  pub commit_custom: Vec<String>,
  pub network_fee: u64,
  pub service_fee: u64,
  pub commit_vsize: u64,
  pub commit_fee: u64,
}

impl Cancel {
  pub fn build(
    self,
    options: Options,
    service_address: Option<Address>,
    service_fee: Option<Amount>,
    _mysql: Option<Arc<MysqlDatabase>>,
  ) -> Result<Output> {
    if !self.source.is_valid_for_network(options.chain().network()) {
      bail!(
        "Address `{}` is not valid for {}",
        self.source,
        options.chain()
      );
    }

    // check address types, only support p2tr and p2wpkh
    let address_type = if let Some(address_type) = self.source.address_type() {
      if (address_type == AddressType::P2tr) || (address_type == AddressType::P2wpkh) {
        address_type
      } else {
        bail!(
          "Address type `{}` is not valid, only support p2tr and p2wpkh",
          address_type
        );
      }
    } else {
      bail!(
        "Address `{}` is not valid for {}",
        self.source,
        options.chain()
      );
    };

    log::info!("Open index...");
    let index = Index::read_open(&options, true)?;
    // index.update()?;

    log::info!("Get utxo...");
    let cancel_unspent_outputs = index.get_unspent_outputs_by_outpoints(&self.inputs, self.default_amount)?;

    let mut all_unspent_outputs = index
      .get_unspent_outputs_by_mempool_v2(&format!("{}", self.source), BTreeMap::new())
      .unwrap_or(BTreeMap::new());
    all_unspent_outputs.extend(cancel_unspent_outputs.clone());

    let mut service_fee = service_fee.unwrap_or(Amount::ZERO).to_sat();
    if service_address.is_none() {
      service_fee = 0;
    }

    let output = if service_fee == 0 {
      vec![TxOut {
        script_pubkey: self.destination.script_pubkey(),
        value: 0,
      }]
    } else {
      vec![
        TxOut {
          script_pubkey: self.destination.script_pubkey(),
          value: 0,
        },
        TxOut {
          script_pubkey: service_address.unwrap().script_pubkey(),
          value: service_fee,
        },
      ]
    };

    let (mut cancel_tx, mut network_fee) = Self::build_cancel_transaction(
      self.fee_rate,
      self.inputs.clone(),
      output.clone(),
      address_type,
    );

    let mut commit_vsize = cancel_tx.vsize() as u64;

    let mut input_amount = Self::get_amount(&cancel_tx, &all_unspent_outputs)?;

    let mut need_amount = 0;
    if input_amount <= network_fee {
      need_amount = network_fee - input_amount;
    }

    if need_amount > 0 {
      let mut diff_unspent_outputs: BTreeMap<OutPoint, Amount> = BTreeMap::new();
      for (key, value) in &all_unspent_outputs {
        if !cancel_unspent_outputs.contains_key(key) {
          diff_unspent_outputs.insert(*key, *value);
        }
      }

      let mut additional_inputs: Vec<OutPoint> = vec![];

      let mut entries: Vec<(OutPoint, Amount)> =
        diff_unspent_outputs.iter().map(|(o, a)| (*o, *a)).collect();
      entries.sort_by(|a, b| b.1.cmp(&a.1));

      let mut cur_amounts = 0;
      let mut next_index = 0;
      for (outpoint, amount) in &entries {
        if cur_amounts >= need_amount {
          break;
        }
        cur_amounts += amount.to_sat();
        additional_inputs.push(*outpoint);
        next_index += 1;
      }
      if next_index + 1 < entries.len() {
        additional_inputs.push(entries[next_index].0);
        next_index += 1;
      }

      if next_index + 1 < entries.len() {
        additional_inputs.push(entries[next_index].0);
        next_index += 1;
      }
      additional_inputs.extend(self.inputs.clone());
      (cancel_tx, network_fee) =
        Self::build_cancel_transaction(self.fee_rate, additional_inputs, output, address_type);

      commit_vsize = cancel_tx.vsize() as u64;

      input_amount = Self::get_amount(&cancel_tx, &all_unspent_outputs)?;

      if input_amount <= network_fee {
        bail!("Input amount less than network fee, has search next two");
      }
    }

    cancel_tx.output[0].value = input_amount - network_fee;
    for input in &mut cancel_tx.input {
      input.witness = Witness::new();
    }

    let unsigned_transaction_psbt = Self::get_psbt(&cancel_tx, &all_unspent_outputs, &self.source)?;
    let unsigned_commit_custom = Self::get_custom(&unsigned_transaction_psbt);

    log::info!("Build cancel success");

    Ok(Output {
      transaction: serialize_hex(&unsigned_transaction_psbt),
      commit_custom: unsigned_commit_custom,
      network_fee,
      service_fee,
      commit_vsize,
      commit_fee: network_fee,
    })
  }

  pub fn run(self, options: Options) -> Result {
    print_json(self.build(options, None, None, None)?)?;
    Ok(())
  }

  fn get_amount(tx: &Transaction, utxos: &BTreeMap<OutPoint, Amount>) -> Result<u64> {
    let mut amount = 0;
    for i in 0..tx.input.len() {
      amount += utxos
        .get(&tx.input[i].previous_output)
        .ok_or_else(|| anyhow!("wallet contains no cardinal utxos"))?
        .to_sat();
    }
    Ok(amount)
  }

  fn get_psbt(
    tx: &Transaction,
    utxos: &BTreeMap<OutPoint, Amount>,
    source: &Address,
  ) -> Result<Psbt> {
    let mut tx_psbt = Psbt::from_unsigned_tx(tx.clone())?;
    for i in 0..tx_psbt.unsigned_tx.input.len() {
      tx_psbt.inputs[i].witness_utxo = Some(TxOut {
        value: utxos
          .get(&tx_psbt.unsigned_tx.input[i].previous_output)
          .ok_or_else(|| anyhow!("wallet contains no cardinal utxos"))?
          .to_sat(),
        script_pubkey: source.script_pubkey(),
      });
    }
    Ok(tx_psbt)
  }

  fn get_custom(tx: &Psbt) -> Vec<String> {
    let unsigned_commit_custom = ConstructTransaction {
      pre_outputs: TransactionOutputArray {
        outputs: tx
          .inputs
          .iter()
          .map(|v| v.witness_utxo.clone().expect("Must has input"))
          .collect(),
      },
      cur_transaction: tx.unsigned_tx.clone(),
    };

    let mut result: Vec<String> = vec![serialize_hex(&unsigned_commit_custom)];
    for v in tx.unsigned_tx.input.iter() {
      result.push(format!("{}", v.previous_output.txid));
      result.push(v.previous_output.vout.to_string())
    }

    result
  }

  fn build_cancel_transaction(
    fee_rate: FeeRate,
    input: Vec<OutPoint>,
    output: Vec<TxOut>,
    input_type: AddressType,
  ) -> (Transaction, u64) {
    let witness_size = if input_type == AddressType::P2tr {
      TransactionBuilder::SCHNORR_SIGNATURE_SIZE
    } else {
      TransactionBuilder::P2WPKH_WINETSS_SIZE
    };

    let cancel_tx = Transaction {
      input: input
        .iter()
        .map(|item| TxIn {
          previous_output: *item,
          script_sig: script::Builder::new().into_script(),
          witness: Witness::from_vec(vec![vec![0; witness_size]]),
          sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
        })
        .collect(),
      output,
      lock_time: PackedLockTime::ZERO,
      version: 1,
    };

    let fee = fee_rate.fee(cancel_tx.vsize());
    (cancel_tx, fee.to_sat())
  }
}
