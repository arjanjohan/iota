// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

import { useQuery } from '@tanstack/react-query';
import { normalizeIotaAddress } from '@iota/iota-sdk/utils';
import { getInactiveValidatorsData } from '../../utils';
import { useIotaClient } from '@iota/dapp-kit';

export function useGetInactiveValidatorData(
    inactivePoolsId?: string,
    validatorAddress?: string,
){
    const client = useIotaClient();
    return useQuery({
        queryKey: [inactivePoolsId, validatorAddress],
        async queryFn() {
            if (!inactivePoolsId || !validatorAddress) {
                throw Error('Missing params');
            }
            const inactiveValidators = await client.getDynamicFields({
                parentId: normalizeIotaAddress(inactivePoolsId),
            });

            const pendingInactiveValidatorsData = await Promise.all(
                inactiveValidators.data.map(
                    async (validator) =>
                        await getInactiveValidatorsData(client, validator.objectId),
                ),
            );

            return pendingInactiveValidatorsData;
        },
        enabled: !!inactivePoolsId && !!validatorAddress,
        select(validators) {
            return validators.find((validator) => validator?.validatorAddress === validatorAddress);
        },
    });
}
