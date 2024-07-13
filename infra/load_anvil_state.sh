#!/bin/bash

cat infra/anvil_state.txt | xargs cast rpc anvil_loadState
