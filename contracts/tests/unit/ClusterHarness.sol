// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import {Cluster} from "src/Cluster.sol";

/// @notice Test harness to expose internal functions for testing
contract ClusterHarness is Cluster {   
    function exposed_validateOperatorData(bytes memory data) external view {
        _validateOperatorData(data);
    } 
    
    function exposed_isSlotInKeyspace(uint8 slot, uint8 keyspaceIndex) external view returns (bool) {
        return _isSlotInKeyspace(slot, keyspaceIndex);
    }
} 
