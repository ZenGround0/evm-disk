# evm-disk

Mount any evm smart contract onto filecoin network storage

# System Overview

## Storage contract

- Simple demo contract
  1. On chain data prep and onramp communication
  2. Calldata + inclusion proofs for reads
- `mount` tool for rewriting evm bytecode to automatically do dataprep + onramp storage and calldata + inclusion proof reads

## Fil Block Device 
- Translate between evm storage slots and filecoin data commps
1. Preplay transactions to discover the needed slots (in practice include all slots in calldata already? And then record? ) for preparing reads
2. Postplay the transaction to get the data written at slots

(1) is for software that prepares Txs with calldata and inclusion proofs from FIL
(2) is for the blockchain buffer that fetches data for onramp deal making using only a cross chain fullnode!
Both are expounded upon below

### Prepare Tx

Run by people who want to call the contract.

Software that prepares a tx for a mounted contract.  It looks up data based on slot (perhaps by stacking calldata with everything, perhaps by hackiliciously modifying calldata in place to update it with queries to fil), creates an inclusion proof to a particular array entry in commp block device on chain storage.  Then creates the tx with the exactly correct calldata.

### Blockchain buffer

Run by people who want to get paid storing contract bytes.

Replay in order to get full byte values beneath commps in order to make a deal with filecoin network.  This is a dropin replacement of the [onramp http buffer](https://github.com/ZenGround0/onramp-contracts/blob/main/contract-tools/xchain/buffer_http.go).  It has the very nice property of having 100% availability since its DA comes from blockchain consensus.

## Onramp 

Run by people who want to get paid storing contract bytes

We'll want a simplified and UX improved onramp system to demonstrate that this works in real time
1. Swap PoRep for PDP.
   a. xchain deal making should now be PDP RPC call, no more deal protocol
   b. Prover contract is now a [PDP Service](https://github.com/FILCAT/pdp/blob/main/contracts/src/SimplePDPService.sol) replacement i.e. CrossChainPDPService.
   c. Need to figure out PDP SP transfer protocol, maybe its sync or maybe async http transfer works just as well
2. Remove aggregation
   a. this will simplify [xchain code](https://github.com/ZenGround0/onramp-contracts/blob/main/contract-tools/xchain/xchain.go#L491-L613) and operations
   b. No more CAR header shenanigans
3. Add in a blockchain buffer module to run in xchain daemon 

All of this can be done straightforwardly starting with a fork of [Onramp code](https://github.com/ZenGround0/onramp-contracts/tree/main) 


# Prototype Deployment strategy

To deploy a prototype we'll want 

Contracts
* demo and onramp contracts on arbitrum sepolia or some other cheap Eth L2
* CrossChainPDPService contract deployed on filecoin
Nodes
* a PDP Storage provider
* arbitrum sepolia fullnode for blockchain buffer
* xchain bridge to make PDP deals

Preparing Txs will call PDP retrieval from the PDP SP and the filecoin block device for tx replay but in an ephemeral client process
For the full mounted contract milestone contract preparation will happen once before contract deploy 




