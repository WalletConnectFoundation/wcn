// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

struct Node {
    uint256 id;
    bytes data;
}

struct Nodes {
    Node[] slots;
    mapping(uint256 => uint8) indexes;
    uint8[] freeSlotIndexes;
}

library NodesLib {
    function set(Nodes storage self, Node memory node) internal {
        require(node.id != 0, "invalid id");

        uint8 idx;

        if (self.slots.length > 0) {
            idx = self.indexes[node.id];
            if (idx != 0 || self.slots[0].id == node.id) {
                self.slots[idx] = node;
                return;
            } 
        }

        if (self.freeSlotIndexes.length > 0) {
            idx = self.freeSlotIndexes[self.freeSlotIndexes.length - 1];
            self.freeSlotIndexes.pop();
        } else {
            require(self.slots.length < 256, "too many nodes");
            idx = uint8(self.slots.length);
            self.slots.push();
        }

        self.indexes[node.id] = idx; 
        self.slots[idx] = node;
    }

    function remove(Nodes storage self, uint256 id) internal {
        require(id != 0, "invalid id");
    
        uint8 idx = self.indexes[id];
        require((idx != 0 || self.slots[0].id == id), "node doesn't exist");

        delete self.slots[idx];
        self.freeSlotIndexes.push(idx);
    }

    function length(Nodes storage self) internal view returns (uint256) {
        return self.slots.length - self.freeSlotIndexes.length;
    }

    function getView(Nodes storage self) internal view returns (Node[] memory) {
        Node[] memory nodes = new Node[](length(self));       
        uint256 j;
        for (uint256 i = 0; i < self.slots.length; i++) {
            if (self.slots[i].id != 0) {
                nodes[j] = Node({
                    id: self.slots[i].id,
                    data: self.slots[i].data
                });
                j++;
            }
        }
        return nodes;
    }
}

