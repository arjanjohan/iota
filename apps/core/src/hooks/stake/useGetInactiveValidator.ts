// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

import { useQuery } from '@tanstack/react-query';
import { normalizeIotaAddress } from '@iota/iota-sdk/utils';
import { getInactiveValidatorsData } from '../../utils';
import { useIotaClient, useIotaClientQuery } from '@iota/dapp-kit';

export function useGetInactiveValidator(validatorAddress?: string) {
    const client = useIotaClient();
    const { data: systemStateData } = useIotaClientQuery('getLatestIotaSystemState');

    return useQuery({
        queryKey: [systemStateData?.inactivePoolsId, validatorAddress],
        async queryFn() {
            if (!systemStateData?.inactivePoolsId || !validatorAddress) {
                throw Error('Missing params');
            }
            const inactiveValidators = await client.getDynamicFields({
                parentId: normalizeIotaAddress(systemStateData?.inactivePoolsId),
            });

            const pendingInactiveValidatorsData = await Promise.all(
                inactiveValidators.data.map(
                    async (validator) =>
                        await getInactiveValidatorsData(client, validator.objectId),
                ),
            );

            return pendingInactiveValidatorsData;
        },
        enabled: !!systemStateData?.inactivePoolsId && !!validatorAddress,
        select(validators) {
            return validators.find((validator) => validator?.validatorAddress === validatorAddress);
        },
    });
}
