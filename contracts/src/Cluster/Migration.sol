// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "./NodeOperators.sol";

struct Migration {
    address[] operatorsToRemove;
    NewNodeOperator[] operatorsToAdd;

    mapping(address => bool) pullingOperators;
    uint8 pullingOperatorsCount;
}

library MigrationLib {
    using MigrationLib for Migration;

    function start(
        Migration storage self,
        NodeOperator[] storage currentOperators,
        address[] calldata operatorsToRemove,
        NewNodeOperator[] calldata operatorsToAdd
    ) public {
        require(!inProgress(self), "migration already in progress");
        require(operatorsToRemove.length > 0 || operatorsToRemove.length > 0, "nothing to do");

        self.operatorsToRemove = operatorsToRemove;
        self.operatorsToAdd = operatorsToAdd;

        for (uint256 i = 0; i < currentOperators.length; i++) {
            if (currentOperators[i].addr != address(0)) {
                self.pullingOperators[currentOperators[i].addr] = true;
                self.pullingOperatorsCount++;
            }
        }

        for (uint256 i = 0; i < operatorsToRemove.length; i++) {
            self.operatorsToRemove.push(operatorsToRemove[i]);
            self.pullingOperators[operatorsToRemove[i]] = false;
            self.pullingOperatorsCount--;
        }

        for (uint256 i = 0; i < operatorsToAdd.length; i++) {
            self.operatorsToAdd.push(operatorsToAdd[i]);
            self.pullingOperators[operatorsToAdd[i].addr] = true;
            self.pullingOperatorsCount++;
        }
    }

    function completeDataPull(Migration storage self, address operator) public {
        require(self.pullingOperators[operator], "already completed");
        self.pullingOperators[operator] = false;
        self.pullingOperatorsCount--;
    }

    function complete(Migration storage self) public {
        require(inProgress(self), "not in progress");
        require(self.pullingOperatorsCount == 0, "data pull in progress");
        self.cleanup();
    }

    function abort(Migration storage self) public {
        require(inProgress(self), "not in progress");
        self.cleanup();
    }

    function cleanup(Migration storage self) internal {
        delete self.operatorsToRemove;
        delete self.operatorsToAdd;
        delete self.pullingOperatorsCount;
    }

    function inProgress(Migration storage self) public view returns (bool) {
        return (self.operatorsToRemove.length != 0 || self.operatorsToAdd.length != 0);
    }
}
