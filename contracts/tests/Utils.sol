// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import {Keyspace, Bitmask} from "src/Cluster.sol";

function keyspace(uint8[] memory operators, uint8 replicationStrategy) returns (Keyspace memory) {
    return Keyspace({
        operatorBitmask: Bitmask.fromArray(operators),
        replicationStrategy: replicationStrategy
    });
}
