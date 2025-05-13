// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import './Nodes.sol';
import './NodeOperators.sol';
import './Migration.sol';
import './Maintenance.sol';

struct Settings {
    uint8 minOperators;
    uint8 minNodes;
}

struct ClusterView {
    NodeOperatorsView operators;
    MigrationView migration;
    Maintenance maintenance;

    uint64 keyspaceVersion;
    uint128 version;
}


event MigrationStarted(address[] operatorsToRemove, NodeOperatorView[] operatorsToAdd, uint128 version);
event MigrationDataPullCompleted(address operator, uint128 version);
event MigrationCompleted(uint128 version);
event MigrationAborted(uint128 version);

event MaintenanceStarted(address operator, uint128 version);
event MaintenanceCompleted(address operator, uint128 version);
event MaintenanceAborted(uint128 version);

event NodeSet(address operator, Node node, uint128 version);
event NodeRemoved(address operator, uint256 id, uint128 version);

contract Cluster {
    using NodeOperatorsLib for NodeOperators;
    using MigrationLib for Migration;
    using MaintenanceLib for Maintenance;

    address owner;

    Settings settings;
    NodeOperators operators;
    Migration migration;
    Maintenance maintenance;

    uint64 keyspaceVersion;
    uint128 version;

    constructor(Settings memory initialSettings, NodeOperatorView[] memory initialOperators) {
        owner = msg.sender;
        settings = initialSettings;
    
        validateOperatorsCount(initialOperators.length);
    
        for (uint256 i = 0; i < initialOperators.length; i++) {
            validateNodesCount(initialOperators[i].nodes.length);
            operators.add(initialOperators[i]);
        }
    }

    modifier onlyOwner() {
        require(msg.sender == owner, "not the owner");
        _;
    }

    modifier onlyOperator() {
        require(operators.exists(msg.sender), "not an operator");
        _;
    }

    function startMigration(address[] calldata operatorsToRemove, NodeOperatorView[] calldata operatorsToAdd) external onlyOwner {
        require(!maintenance.inProgress(), "maintenance in progress");

        for (uint256 i = 0; i < operatorsToRemove.length; i++) {
            require(operators.exists(operatorsToRemove[i]), "unknown operator");
        }

        for (uint256 i = 0; i < operatorsToAdd.length; i++) {
            validateNodesCount(operatorsToAdd[i].nodes.length);
            require(!operators.exists(operatorsToAdd[i].addr), "operator already exists");
        }

        validateOperatorsCount((operators.length() - operatorsToRemove.length + operatorsToAdd.length));

        migration.start(operators.slots, operatorsToRemove, operatorsToAdd);
        version++;
        emit MigrationStarted(operatorsToRemove, operatorsToAdd, version);
    }

    function completeMigration() external {
        migration.completeDataPull(msg.sender);
        version++;
        emit MigrationDataPullCompleted(msg.sender, version);

        if (migration.pullingOperatorsCount > 0) {
            return;
        }

        for (uint256 i = 0; i < migration.operatorsToRemove.length; i++) {
            operators.remove(migration.operatorsToRemove[i]);
        }

        for (uint256 i = 0; i < migration.operatorsToAdd.length; i++) {
            operators.add(migration.operatorsToAdd[i]);
        }

        keyspaceVersion++;

        migration.complete();
        version++;

        emit MigrationCompleted(version);
    }

    function abortMigration() external onlyOwner {
        migration.abort();        
        version++;
        emit MigrationAborted(version);
    }

    function startMaintenance() external onlyOperator {
        require(!migration.inProgress(), "migration in progress");

        maintenance.start(msg.sender);
        version++;
        emit MaintenanceStarted(msg.sender, version);
    }

    function completeMaintenance() external onlyOperator {
        maintenance.complete(msg.sender);
        version++;
        emit MaintenanceCompleted(msg.sender, version);
    }

    function abortMaintenance() external onlyOwner {
        maintenance.abort();
        version++;
        emit MaintenanceAborted(version);
    }

    function setNode(Node calldata node) external onlyOperator {
        operators.setNode(msg.sender, node);
        version++;
        emit NodeSet(msg.sender, node, version);
    }

    function removeNode(uint256 id) external onlyOperator {
        operators.removeNode(msg.sender, id);
        require(operators.nodesCount(msg.sender) >= settings.minNodes, "too few nodes");
        version++;
        emit NodeRemoved(msg.sender, id, version);
    }

    function updateSettings(Settings calldata newSettings) external onlyOwner {
        settings = newSettings;
    }

    function transferOwnership(address newOwner) external onlyOwner {
        owner = newOwner;
    }

    function validateOperatorsCount(uint256 value) view internal {
        require(value >= settings.minOperators, "too few operators");
        require(value <= 256, "too many operators");
    }

    function validateNodesCount(uint256 value) view internal {
        require(value >= settings.minNodes, "too few nodes");
        require(value <= 256, "too many nodes");
    }

    function getView() public view returns (ClusterView memory) {
        return ClusterView({
            operators: operators.getView(),
            migration: migration.getView(operators.slots),
            maintenance: maintenance,
            keyspaceVersion: keyspaceVersion,
            version: version
        });
    }
}
