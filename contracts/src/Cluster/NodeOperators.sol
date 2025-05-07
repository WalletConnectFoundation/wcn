// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import './Nodes.sol';

struct NodeOperator {
    address addr;
    Nodes nodes;
    bytes data;
}

struct NodeOperatorView {
    address addr;
    Node[] nodes;
    bytes data;
}

struct NodeOperators {
    NodeOperator[] slots;
    mapping(address => uint8) indexes;
    uint8[] freeSlotIndexes;
}

struct NodeOperatorsView {
    NodeOperatorView[] slots;
}

library NodeOperatorsLib {
    using NodesLib for Nodes;
    using NodeOperatorsLib for NodeOperators;

    function add(NodeOperators storage self, NodeOperatorView calldata operator) public {
        require(!exists(self, operator.addr), "operator already exists");

        uint8 idx;
    
        if (self.freeSlotIndexes.length > 0) {
            idx = self.freeSlotIndexes[self.freeSlotIndexes.length - 1];
            self.freeSlotIndexes.pop();
        } else {
            require(self.slots.length < 256, "too many operators");
            idx = uint8(self.slots.length);
            self.slots.push();
        }

        self.indexes[operator.addr] = idx;
        NodeOperator storage op = self.slots[idx];
        op.addr = operator.addr;
        op.data = operator.data;
        for (uint256 i = 0; i < operator.nodes.length; i++) {
            op.nodes.set(operator.nodes[i]);
        }
    }

    function remove(NodeOperators storage self, address addr) public {
        require(addr != address(0), "invalid address");
    
        uint8 idx = self.indexes[addr];
        require((idx != 0 || self.slots[0].addr == addr), "operator doesn't exist");

        delete self.indexes[addr];
        delete self.slots[idx];
        self.freeSlotIndexes.push(idx);
    }

    function exists(NodeOperators storage self, address addr) public view returns (bool) {
        require(addr != address(0), "invalid address");

        if (self.slots.length > 0) {
            return ((self.indexes[addr] != 0 || self.slots[0].addr == addr));
        }
        
        return false;
    }

    function length(NodeOperators storage self) public view returns (uint8) {
        return uint8(self.slots.length - self.freeSlotIndexes.length);
    }

    function setNode(NodeOperators storage self, address operator, Node calldata node) public {
        self.slots[self.indexes[operator]].nodes.set(node);
    }

    function removeNode(NodeOperators storage self, address operator, uint256 id) public {
        self.slots[self.indexes[operator]].nodes.remove(id);
    }

    function getView(NodeOperators storage self) public view returns (NodeOperatorsView memory) {
        NodeOperatorView[] memory slots = new NodeOperatorView[](length(self));
        for (uint256 i = 0; i < self.slots.length; i++) {
            slots[i] = NodeOperatorView({
                addr: self.slots[i].addr,
                nodes: self.slots[i].nodes.getView(),
                data: self.slots[i].data
            });
        }
        return NodeOperatorsView({ slots: slots });
    }
}
