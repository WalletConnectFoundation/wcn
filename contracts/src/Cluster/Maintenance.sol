// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

struct Maintenance {
    address slot;
}

library MaintenanceLib {
    function start(Maintenance storage self, address operator) public {
        require(operator != address(0), "invalid address");
        require(self.slot == address(0), "already occupied");
        self.slot = operator;
    }

    function finish(Maintenance storage self, address operator) public {
        require(operator != address(0), "invalid address");
        require(self.slot == operator, "not occupied");
        delete self.slot;
    }

    function inProgress(Maintenance calldata self) public pure returns (bool) {
        return (self.slot != address(0));
    }
}
