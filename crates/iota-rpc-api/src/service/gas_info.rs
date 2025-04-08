// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2024 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use crate::{
    Result, RpcService,
    proto::node::v2alpha::{GetGasInfoRequest, GetGasInfoResponse},
};

impl RpcService {
    pub fn get_gas_info(&self, _request: GetGasInfoRequest) -> Result<GetGasInfoResponse> {
        let reference_gas_price = self.reader.get_system_state_summary()?.reference_gas_price;

        Ok(GetGasInfoResponse {
            reference_gas_price: Some(reference_gas_price),
        })
    }
}
