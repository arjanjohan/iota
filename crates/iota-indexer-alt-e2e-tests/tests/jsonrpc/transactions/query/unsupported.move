// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2024 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//# init --protocol-version 70 --addresses test=0x0 --simulator

//# create-checkpoint

//# run-jsonrpc
{
  "method": "iotax_queryTransactionBlocks",
  "params": [
    {
      "filter": {
        "TransactionKind": "NotSupported"
      }
    }
  ]
}
