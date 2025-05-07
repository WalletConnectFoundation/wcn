// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "./NodeOperators.sol";

struct Migration {
    address[] operatorsToRemove;
    NodeOperatorView[] operatorsToAdd;

    mapping(address => bool) pullingOperators;
    uint8 pullingOperatorsCount;
}

struct MigrationView {
    address[] operatorsToRemove;
    NodeOperatorView[] operatorsToAdd;

    address[] pullingOperators;
}

library MigrationLib {
    using MigrationLib for Migration;

    function start(
        Migration storage self,
        NodeOperator[] storage currentOperators,
        address[] calldata operatorsToRemove,
        NodeOperatorView[] calldata operatorsToAdd
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

    function getView(Migration storage self, NodeOperator[] storage currentOperators) public view returns (MigrationView memory) {
        address[] memory toRemove = new address[](self.operatorsToRemove.length);
        for (uint256 i = 0; i < self.operatorsToRemove.length; i++) {
            toRemove[i] = self.operatorsToRemove[i];
        }

        NodeOperatorView[] memory toAdd = new NodeOperatorView[](self.operatorsToAdd.length);
        for (uint256 i = 0; i < self.operatorsToAdd.length; i++) {
            toAdd[i] = self.operatorsToAdd[i];
        }

        address[] memory pulling = new address[](self.pullingOperatorsCount);
        uint256 j;
        for (uint256 i = 0; i < currentOperators.length; i++) {
            if (currentOperators[i].addr != address(0) && self.pullingOperators[currentOperators[i].addr]) {
                pulling[j] = currentOperators[i].addr;
                j++;
            }
        }

        return MigrationView({
            operatorsToRemove: toRemove,
            operatorsToAdd: toAdd,
            pullingOperators: pulling
        });
    }
}
