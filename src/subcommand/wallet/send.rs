use bitcoin::AddressType;

use {super::*, crate::wallet::Wallet};

#[derive(Debug, Parser)]
pub(crate) struct Send {
  address: Address,
  outgoing: Outgoing,
  #[clap(long, help = "Use fee rate of <FEE_RATE> sats/vB")]
  fee_rate: FeeRate,
}

#[derive(Serialize, Deserialize)]
pub struct Output {
  pub transaction: Txid,
}

impl Send {
  pub(crate) fn run(self, options: Options) -> Result {
    if !self.address.is_valid_for_network(options.chain().network()) {
      bail!(
        "Address `{}` is not valid for {}",
        self.address,
        options.chain()
      );
    }

    // check address types, only support p2tr and p2wpkh
    let address_type = if let Some(address_type) = self.address.address_type() {
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
        self.address,
        options.chain()
      );
    };

    let index = Index::open(&options)?;
    index.update()?;

    let client = options.bitcoin_rpc_client_for_wallet_command(false)?;

    let unspent_outputs = index.get_unspent_outputs(Wallet::load(&options)?)?;

    let inscriptions = index.get_inscriptions(None)?;

    let satpoint = match self.outgoing {
      Outgoing::SatPoint(satpoint) => {
        for inscription_satpoint in inscriptions.keys() {
          if satpoint == *inscription_satpoint {
            bail!("inscriptions must be sent by inscription ID");
          }
        }
        satpoint
      }
      Outgoing::InscriptionId(id) => index
        .get_inscription_satpoint_by_id(id)?
        .ok_or_else(|| anyhow!("Inscription {id} not found"))?,
      Outgoing::Amount(amount) => {
        let all_inscription_outputs = inscriptions
          .keys()
          .map(|satpoint| satpoint.outpoint)
          .collect::<HashSet<OutPoint>>();

        let wallet_inscription_outputs = unspent_outputs
          .keys()
          .filter(|utxo| all_inscription_outputs.contains(utxo))
          .cloned()
          .collect::<Vec<OutPoint>>();

        if !client.lock_unspent(&wallet_inscription_outputs)? {
          bail!("failed to lock ordinal UTXOs");
        }

        let txid =
          client.send_to_address(&self.address, amount, None, None, None, None, None, None)?;

        print_json(Output { transaction: txid })?;

        return Ok(());
      }
    };

    let change = [get_change_address(&client)?, get_change_address(&client)?];

    let unsigned_transaction = TransactionBuilder::build_transaction_with_postage(
      address_type,
      satpoint,
      inscriptions,
      unspent_outputs,
      self.address,
      change,
      self.fee_rate,
    )?;

    let signed_tx = client
      .sign_raw_transaction_with_wallet(&unsigned_transaction, None, None)?
      .hex;

    let txid = client.send_raw_transaction(&signed_tx)?;

    println!("{txid}");

    Ok(())
  }
}
