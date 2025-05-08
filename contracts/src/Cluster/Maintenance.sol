// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

struct Maintenance {
    address slot;
}

library MaintenanceLib {
    function start(Maintenance storage self, address operator) public {
        require(operator != address(0), "invalid address");
        require(self.slot == address(0), "another maintenance in progress");
        self.slot = operator;
    }

    function complete(Maintenance storage self, address operator) public {
        require(operator != address(0), "invalid address");
        require(self.slot == operator, "not under maintenance");
        delete self.slot;
    }

    function abort(Maintenance storage self) public {
        require(self.slot != address(0), "not under maintenance");
        delete self.slot;
    }

    function inProgress(Maintenance calldata self) public pure returns (bool) {
        return (self.slot != address(0));
    }
}
