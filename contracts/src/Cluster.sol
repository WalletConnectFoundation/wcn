// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import {Ownable2StepUpgradeable} from "@openzeppelin/contracts-upgradeable/access/Ownable2StepUpgradeable.sol";
import {UUPSUpgradeable} from "@openzeppelin/contracts/proxy/utils/UUPSUpgradeable.sol";
import {LibSort} from "solady/utils/LibSort.sol";

struct Settings {
    uint16 maxOperatorDataBytes;
    uint8 minOperators;
}

struct NodeOperator {
    address addr;
    bytes data;
}

struct Keyspace {
    uint8[] members;
    uint8 replicationStrategy;
}

/// @notice Represents the maintenance state of the cluster
/// @dev Used to track whether the cluster is in maintenance mode and who initiated it
/// @param slot The address of the operator or owner who initiated maintenance. address(0) means no maintenance is active
/// @param isOwnerMaintenance True if maintenance was initiated by the owner, false if by an operator
struct MaintenanceState {
    address slot;
    bool isOwnerMaintenance;
}

struct ClusterView {
    address owner;
    Settings settings;
    uint128 version;
    uint64 keyspaceVersion;
    uint16 operatorCount;
    address[] operators;
    bytes[] operatorData;
    Keyspace[2] keyspaces;
    uint64 migrationId;
    uint8[] pullingOperators;
    MaintenanceState maintenance;
}

contract Cluster is Ownable2StepUpgradeable, UUPSUpgradeable {
    using LibSort for *;

    /*//////////////////////////////////////////////////////////////////////////
                            CONSTANTS & ERRORS
    //////////////////////////////////////////////////////////////////////////*/

    // Maximum number of operators per cluster
    uint16 public constant MAX_OPERATORS = 256;

    error Unauthorized();
    error MigrationInProgress();
    error NoMigrationInProgress();
    error InvalidOperator();
    error OperatorNotFound();
    error OperatorNotPulling();
    error CallerNotOperator();
    error OperatorExists();
    error OperatorInKeyspace();
    error SameKeyspace();
    error WrongMigrationId();
    error InvalidOperatorData();
    error TooManyOperators();
    error InsufficientOperators();
    error MaintenanceInProgress();

    /*//////////////////////////////////////////////////////////////////////////
                                EVENTS
    //////////////////////////////////////////////////////////////////////////*/

    event ClusterInitialized(NodeOperator[] operators, Settings settings, uint128 version);
    event MigrationStarted(uint64 indexed id, uint8[] operators, uint8 replicationStrategy, uint64 keyspaceVersion, uint128 version);
    event MigrationDataPullCompleted(uint64 indexed id, address indexed operator, uint128 version);
    event MigrationCompleted(uint64 indexed id, address indexed operator, uint128 version);
    event MigrationAborted(uint64 indexed id, uint128 version);

    event NodeOperatorAdded(address indexed operator, uint8 indexed slot, bytes operatorData, uint128 version);
    event NodeOperatorUpdated(address indexed operator, uint8 indexed slot, bytes operatorData, uint128 version);
    event NodeOperatorRemoved(address indexed operator, uint8 indexed slot, uint128 version);

    event MaintenanceToggled(address indexed operator, bool active, uint128 version);
    event SettingsUpdated(Settings newSettings, address indexed updatedBy, uint128 version);

    /*//////////////////////////////////////////////////////////////////////////
                            STORAGE
    //////////////////////////////////////////////////////////////////////////*/

    Settings public settings;
    uint128 public version;
    
    // Slot-based operator storage (stable u8 indexing)
    mapping(uint8 => address) public operatorSlots; // slot => operator address
    mapping(uint8 => bool) public slotOccupied; // which slots are occupied
    mapping(address => uint8) public operatorToSlot; // operator address => slot
    mapping(address => bytes) public operatorData; // operator data

    uint16 public operatorCount;
    
    // Keyspaces
    Keyspace[2] public keyspaces;
    uint64 public keyspaceVersion;
    
    // Migration
    uint64 public migrationId;
    mapping(uint8 => bool) public slotPulling;
    uint16 public pullingCount;

    // Maintenance mutex
    MaintenanceState public maintenance;

    // Gas optimization: cache next free slot for O(1) operator addition
    uint8 public nextFreeSlot;

    // Storage gap for future variables (reserves space for rewards layer)
    uint256[45] private __gap;

    /*//////////////////////////////////////////////////////////////////////////
                            CONSTRUCTOR & INITIALIZERf
    //////////////////////////////////////////////////////////////////////////*/

    constructor() {
        _disableInitializers();
    }

    function initialize(Settings memory initialSettings, NodeOperator[] memory initialOperators) external initializer {
        if (initialOperators.length > MAX_OPERATORS) revert TooManyOperators();
        if (initialOperators.length < initialSettings.minOperators) revert InsufficientOperators();
        
        __Ownable2Step_init();
        __Ownable_init(msg.sender);
        
        settings = initialSettings;
        
        // Allocate slots for initial operators
        for (uint256 i = 0; i < initialOperators.length;) {
            NodeOperator memory op = initialOperators[i];
            _validateOperatorData(op.data);
            
            uint8 slot = uint8(i); // Use sequential slots for initial operators
            if (operatorSlots[slot] != address(0)) revert OperatorExists();
            
            operatorSlots[slot] = op.addr;
            slotOccupied[slot] = true;
            operatorToSlot[op.addr] = slot;
            operatorData[op.addr] = op.data;
            
            unchecked { ++i; }
        }
        operatorCount = uint16(initialOperators.length);
        
        // Initialize next free slot
        nextFreeSlot = uint8(initialOperators.length);
        
        // Initialize first keyspace with all operator slots (sorted)
        uint8[] memory operatorSlotsList = new uint8[](initialOperators.length);
        for (uint256 i = 0; i < initialOperators.length;) {
            operatorSlotsList[i] = uint8(i);
            unchecked { ++i; }
        }
        keyspaces[0].members = operatorSlotsList;

        emit ClusterInitialized(initialOperators, settings, ++version);
    }

    /*//////////////////////////////////////////////////////////////////////////
                            MODIFIERS
    //////////////////////////////////////////////////////////////////////////*/


    /*//////////////////////////////////////////////////////////////////////////
                            MIGRATION FUNCTIONS
    //////////////////////////////////////////////////////////////////////////*/

    function startMigration(uint8[] calldata newOperatorSlots, uint8 replicationStrategy) external onlyOwner {
        if (maintenance.slot != address(0)) revert MaintenanceInProgress();
        if (pullingCount > 0) revert MigrationInProgress();
        if (newOperatorSlots.length > MAX_OPERATORS) revert TooManyOperators();
        
        // Validate operator slots are sorted, unique, and exist
        if (newOperatorSlots.length > 1) {
            for (uint256 i = 1; i < newOperatorSlots.length;) {
                if (newOperatorSlots[i] <= newOperatorSlots[i-1]) revert InvalidOperator();
                unchecked { ++i; }
            }
        }
        
        for (uint256 i = 0; i < newOperatorSlots.length;) {
            uint8 slot = newOperatorSlots[i];
            if (!slotOccupied[slot]) revert OperatorNotFound();
            slotPulling[slot] = true;
            unchecked { ++i; }
        }
        pullingCount = uint16(newOperatorSlots.length);
        
        // Check if different from current keyspace
        Keyspace storage current = keyspaces[keyspaceVersion % 2];
        if (current.replicationStrategy == replicationStrategy && 
            current.members.length == newOperatorSlots.length) {
            bool same = true;
            for (uint256 i = 0; i < current.members.length;) {
                if (current.members[i] != newOperatorSlots[i]) {
                    same = false;
                    break;
                }
                unchecked { ++i; }
            }
            if (same) revert SameKeyspace();
        }
        
        migrationId++;
        keyspaceVersion++;
        
        // Set new keyspace
        Keyspace storage newKeyspace = keyspaces[keyspaceVersion % 2];
        newKeyspace.members = newOperatorSlots;
        newKeyspace.replicationStrategy = replicationStrategy;
        
        emit MigrationStarted(migrationId, newOperatorSlots, replicationStrategy, keyspaceVersion, ++version);
    }

    function completeMigration(uint64 id) external {
        if (pullingCount == 0) revert NoMigrationInProgress();
        if (id != migrationId) revert WrongMigrationId();
        
        uint8 operatorSlot = operatorToSlot[msg.sender];
        if (operatorSlots[operatorSlot] != msg.sender) revert CallerNotOperator();
        if (!slotPulling[operatorSlot]) revert OperatorNotPulling();
        
        slotPulling[operatorSlot] = false;
        unchecked { --pullingCount; }
        
        if (pullingCount == 0) {
            emit MigrationCompleted(migrationId, msg.sender, ++version);
        } else {
            emit MigrationDataPullCompleted(migrationId, msg.sender, ++version);
        }
    }

    function abortMigration(uint64 id) external onlyOwner {
        if (pullingCount == 0) revert NoMigrationInProgress();
        if (id != migrationId) revert WrongMigrationId();
        
        // Clear pulling state efficiently
        uint8[] memory currentOperatorSlots = keyspaces[keyspaceVersion % 2].members;
        for (uint256 i = 0; i < currentOperatorSlots.length;) {
            slotPulling[currentOperatorSlots[i]] = false;
            unchecked { ++i; }
        }
        pullingCount = 0;
        
        // Revert keyspace
        keyspaceVersion--;
        
        emit MigrationAborted(migrationId, ++version);
    }

    /*//////////////////////////////////////////////////////////////////////////
                            OPERATOR MANAGEMENT
    //////////////////////////////////////////////////////////////////////////*/

    function addNodeOperator(NodeOperator calldata operator) external onlyOwner {
        if (operatorCount >= MAX_OPERATORS) revert TooManyOperators();
        if (operator.addr == address(0)) revert InvalidOperator();
        if (operatorToSlot[operator.addr] != 0 || operatorSlots[0] == operator.addr) revert OperatorExists();
        
        _validateOperatorData(operator.data);
        
        // Find first available slot using cached value
        uint8 slot = _findAvailableSlot();
        
        operatorSlots[slot] = operator.addr;
        slotOccupied[slot] = true;
        operatorToSlot[operator.addr] = slot;
        operatorData[operator.addr] = operator.data;
        operatorCount++;
        
        // Update next free slot cache
        _updateNextFreeSlot();
        
        emit NodeOperatorAdded(operator.addr, slot, operator.data, ++version);
    }

    function updateNodeOperatorData(address operatorAddress, bytes calldata newData) external {
        if (msg.sender != operatorAddress && msg.sender != owner()) revert Unauthorized();

        _validateOperatorData(newData);
        uint8 slot = operatorToSlot[operatorAddress];
        if (operatorSlots[slot] != operatorAddress) revert OperatorNotFound();
        
        operatorData[operatorAddress] = newData;
        emit NodeOperatorUpdated(operatorAddress, slot, newData, ++version);
    }

    function removeNodeOperator(address operatorAddr) external onlyOwner {
        uint8 slot = operatorToSlot[operatorAddr];
        if (operatorSlots[slot] != operatorAddr) revert OperatorNotFound();
        if (operatorCount <= settings.minOperators) revert InsufficientOperators();
        
        // Check not in active keyspaces
        if (pullingCount > 0) {
            // During migration, check both keyspaces
            if (_isSlotInKeyspace(slot, 0) || _isSlotInKeyspace(slot, 1)) revert OperatorInKeyspace();
        } else {
            // No migration, check current keyspace
            if (_isSlotInKeyspace(slot, uint8(keyspaceVersion % 2))) revert OperatorInKeyspace();
        }
        
        operatorSlots[slot] = address(0);
        slotOccupied[slot] = false;
        delete operatorToSlot[operatorAddr];
        delete operatorData[operatorAddr];
        operatorCount--;
        
        // Update next free slot cache if this slot is lower
        if (slot < nextFreeSlot) {
            nextFreeSlot = slot;
        }
        
        emit NodeOperatorRemoved(operatorAddr, slot, ++version);
    }

    function setMaintenance(bool active) external {
        if (active) {
            // Starting maintenance - check mutex
            if (maintenance.slot != address(0)) revert MaintenanceInProgress();
            maintenance.slot = msg.sender;
            maintenance.isOwnerMaintenance = (msg.sender == owner());
        } else {
            // Ending maintenance - only the one who started can end it, OR the owner can always end it
            if (maintenance.slot != msg.sender && msg.sender != owner()) revert Unauthorized();
            maintenance.slot = address(0);
            maintenance.isOwnerMaintenance = false;
        }
        
        // Update operator info if it's not the owner
        if (msg.sender != owner()) {
            uint8 slot = operatorToSlot[msg.sender];
            if (operatorSlots[slot] != msg.sender) revert Unauthorized();
        }
        
        emit MaintenanceToggled(msg.sender, active, ++version);
    }

    /*//////////////////////////////////////////////////////////////////////////
                            SETTINGS
    //////////////////////////////////////////////////////////////////////////*/

    function updateSettings(Settings calldata newSettings) external onlyOwner {
        if (operatorCount < newSettings.minOperators) revert InsufficientOperators();
        settings = newSettings;
        emit SettingsUpdated(newSettings, msg.sender, ++version);
    }

    /*//////////////////////////////////////////////////////////////////////////
                            VIEW FUNCTIONS
    //////////////////////////////////////////////////////////////////////////*/

    function getOperatorCount() external view returns (uint16) {
        return operatorCount;
    }

    function getOperatorAt(uint8 slot) external view returns (address) {
        if (!slotOccupied[slot]) revert OperatorNotFound();
        return operatorSlots[slot];
    }

    function isOperator(address addr) external view returns (bool) {
        uint8 slot = operatorToSlot[addr];
        return operatorSlots[slot] == addr;
    }

    function getAllOperators() external view returns (address[] memory) {
        address[] memory operators = new address[](operatorCount);
        uint256 count = 0;
        
        for (uint8 i = 0; i < MAX_OPERATORS && count < operatorCount;) {
            if (slotOccupied[i]) {
                operators[count] = operatorSlots[i];
                unchecked { ++count; }
            }
            unchecked { ++i; }
        }
        
        operators.sort();
        return operators;
    }

    function getCurrentKeyspace() external view returns (uint8[] memory, uint8) {
        Keyspace storage current = keyspaces[keyspaceVersion % 2];
        return (current.members, current.replicationStrategy);
    }

    function getMigrationStatus() external view returns (uint64 id, uint16 remaining, bool inProgress) {
        return (migrationId, pullingCount, pullingCount > 0);
    }

    function getPullingOperators() external view returns (uint8[] memory) {
        if (pullingCount == 0) return new uint8[](0);
        
        uint8[] memory pulling = new uint8[](pullingCount);
        uint8[] memory currentOperatorSlots = keyspaces[keyspaceVersion % 2].members;
        uint256 count = 0;
        
        for (uint256 i = 0; i < currentOperatorSlots.length;) {
            if (slotPulling[currentOperatorSlots[i]]) {
                pulling[count++] = currentOperatorSlots[i];
            }
            unchecked { ++i; }
        }
        
        assembly {
            mstore(pulling, count)
        }
        
        return pulling;
    }


    function getView() external view returns (ClusterView memory) {
        address[] memory operators = this.getAllOperators();
        bytes[] memory opData = new bytes[](operators.length);
        
        for (uint256 i = 0; i < operators.length;) {
            address op = operators[i];
            opData[i] = operatorData[op];
            unchecked { ++i; }
        }
        
        uint8[] memory pullingOps = this.getPullingOperators();
        
        return ClusterView({
            owner: owner(),
            settings: settings,
            version: version,
            keyspaceVersion: keyspaceVersion,
            operatorCount: operatorCount,
            operators: operators,
            operatorData: opData,
            keyspaces: keyspaces,
            migrationId: migrationId,
            pullingOperators: pullingOps,
            maintenance: maintenance
        });
    }

    /// @notice Required override for UUPS proxy implementation
    /// @dev Only the owner can upgrade the implementation
    function _authorizeUpgrade(address) internal override onlyOwner {}

    /*//////////////////////////////////////////////////////////////////////////
                            INTERNAL FUNCTIONS
    //////////////////////////////////////////////////////////////////////////*/

    function _validateOperatorData(bytes memory data) internal view {
        if (data.length == 0) revert InvalidOperatorData();
        if (data.length > settings.maxOperatorDataBytes) revert InvalidOperatorData();
    }

    function _findAvailableSlot() internal view returns (uint8) {
        // Use cached next free slot for O(1) lookup
        if (nextFreeSlot < MAX_OPERATORS && !slotOccupied[uint8(nextFreeSlot)]) {
            return uint8(nextFreeSlot);
        }
        
        // Fallback to linear scan if cache is stale
        for (uint8 i = nextFreeSlot; i < 255;) {
            if (!slotOccupied[i]) {
                return i;
            }
            unchecked { ++i; }
        }
        
        // Scan from beginning if needed
        for (uint8 i = 0; i < nextFreeSlot;) {
            if (!slotOccupied[i]) {
                return i;
            }
            unchecked { ++i; }
        }
        
        revert TooManyOperators();
    }

    function _updateNextFreeSlot() internal {
        // Find next available slot starting from current nextFreeSlot + 1
        for (uint8 i = nextFreeSlot + 1; i < 255;) {
            if (!slotOccupied[i]) {
                nextFreeSlot = i;
                return;
            }
            unchecked { ++i; }
        }
        
        // If no slots found after current position, scan from beginning
        for (uint8 i = 0; i <= nextFreeSlot;) {
            if (!slotOccupied[i]) {
                nextFreeSlot = i;
                return;
            }
            unchecked { ++i; }
        }
        
        // All slots occupied - this should never happen due to operatorCount check
        nextFreeSlot = 255;
    }

    function _isSlotInKeyspace(uint8 slot, uint8 keyspaceIndex) internal view returns (bool) {
        uint8[] memory members = keyspaces[keyspaceIndex].members;
        for (uint256 i = 0; i < members.length;) {
            if (members[i] == slot) return true;
            unchecked { ++i; }
        }
        return false;
    }
}
