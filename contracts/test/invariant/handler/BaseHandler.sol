// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import {Test} from "forge-std/Test.sol";
import {ClusterHarness} from "../../unit/ClusterHarness.sol";
import {ClusterStore} from "../store/ClusterStore.sol";
import {ClusterView} from "../../../src/Cluster.sol";

/// @dev Base contract with common logic needed by all cluster handlers
/// Follows the Lockup pattern for instrumentation and actor management
abstract contract BaseHandler is Test {
    /*//////////////////////////////////////////////////////////////////////////
                                    STATE VARIABLES
    //////////////////////////////////////////////////////////////////////////*/

    /// @dev Maximum number of operators that can be created during an invariant campaign
    uint256 internal constant MAX_OPERATOR_COUNT = 50;

    /// @dev Maps function names to the number of times they have been called
    mapping(string func => uint256 calls) public calls;

    /// @dev The total number of calls made to this contract
    uint256 public totalCalls;

    ClusterHarness public cluster;
    ClusterStore public store;
    address public owner;

    /*//////////////////////////////////////////////////////////////////////////
                                    CONSTRUCTOR
    //////////////////////////////////////////////////////////////////////////*/

    constructor(ClusterHarness _cluster, ClusterStore _store, address _owner) {
        cluster = _cluster;
        store = _store;
        owner = _owner;
    }

    /*//////////////////////////////////////////////////////////////////////////
                                     MODIFIERS
    //////////////////////////////////////////////////////////////////////////*/

    /// @dev Simulates the passage of time. Useful for time-dependent cluster behavior
    /// @param timeJumpSeed A fuzzed value needed for generating random time warps
    modifier adjustTimestamp(uint256 timeJumpSeed) {
        uint256 timeJump = _bound(timeJumpSeed, 1 minutes, 30 days);
        vm.warp(getBlockTimestamp() + timeJump);
        _;
    }

    /// @dev Checks user assumptions for addresses
    modifier checkUsers(address user1, address user2) {
        // Prevent users from being the zero address
        vm.assume(user1 != address(0) && user2 != address(0));
        
        // Prevent the contract itself from playing the role of any user
        vm.assume(user1 != address(this) && user2 != address(this));
        vm.assume(user1 != address(cluster) && user2 != address(cluster));
        vm.assume(user1 != address(store) && user2 != address(store));
        _;
    }

    /// @dev Checks single user assumptions
    modifier checkUser(address user) {
        vm.assume(user != address(0));
        vm.assume(user != address(this));
        vm.assume(user != address(cluster));
        vm.assume(user != address(store));
        _;
    }

    /// @dev Records a function call for instrumentation purposes
    modifier instrument(string memory functionName) {
        calls[functionName]++;
        totalCalls++;
        _;
        // Update store for version tracking (like Lockup tracks status)
        _updateStore();
    }

    /// @dev Makes the owner the caller
    modifier useOwner() {
        resetPrank(owner);
        _;
    }

    /// @dev Makes a random existing operator the caller
    modifier useFuzzedOperator(uint256 operatorSeed) {
        address[] memory operators = cluster.getAllOperators();
        vm.assume(operators.length > 0);
        address selectedOperator = operators[_bound(operatorSeed, 0, operators.length - 1)];
        resetPrank(selectedOperator);
        _;
    }

    /// @dev Makes a valid actor the caller (owner or operator)
    modifier useValidActor(uint256 actorSeed) {
        address[] memory operators = cluster.getAllOperators();
        // Include owner + all operators
        address[] memory validActors = new address[](operators.length + 1);
        validActors[0] = owner;
        for (uint256 i = 0; i < operators.length; i++) {
            validActors[i + 1] = operators[i];
        }
        
        address selectedActor = validActors[_bound(actorSeed, 0, validActors.length - 1)];
        resetPrank(selectedActor);
        _;
    }

    /// @dev Makes a random non-privileged user the caller
    modifier useRandomUser(uint256 userSeed) {
        address randomUser = address(uint160(_bound(userSeed, 0x1000, 0x9999)));
        vm.assume(randomUser != owner);
        vm.assume(!cluster.isOperator(randomUser));
        resetPrank(randomUser);
        _;
    }

    /*//////////////////////////////////////////////////////////////////////////
                                      HELPERS
    //////////////////////////////////////////////////////////////////////////*/

    /// @dev Get current block timestamp
    function getBlockTimestamp() internal view returns (uint40) {
        return uint40(block.timestamp);
    }

    /// @dev Get the current caller address (from prank state)
    /// @return The address of the current caller (pranked address if active, msg.sender otherwise)
    function getCurrentCaller() internal returns (address) {
        (, address currentCaller, ) = vm.readCallers();
        return currentCaller;
    }

    /// @dev Update store with current cluster state (like Lockup's status tracking)
    function _updateStore() internal {
        ClusterView memory currentView = cluster.getView();
        if (!store.isPreviousViewRecorded(currentView.version)) {
            store.updatePreviousViewOf(currentView.version, currentView);
            store.updateIsPreviousViewRecorded(currentView.version);
        }
    }

    function resetPrank(address user) internal {
        vm.stopPrank();
        vm.startPrank(user);
    }
} 