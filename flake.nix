{
  description = "WCN dev environment";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    { self
    , nixpkgs
    , fenix
    , flake-utils
    , ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs {
          inherit system;
          config.allowUnfree = true; # terraform is no longer free
          overlays = [ fenix.overlays.default ];
        };
        fenixPackages = fenix.packages."${system}";
        nativeBuildInputs = with pkgs; [
          pkg-config
          openssl
          clang
        ];
        rustc = {
          stable = fenixPackages.stable.rustc;
          nightly = fenixPackages.minimal.rustc;
        };
        cargo = {
          stable = fenixPackages.stable.cargo;
          nightly = fenixPackages.minimal.cargo;
        };
        rust-std = {
          stable = fenixPackages.stable.rust-std;
          nightly = fenixPackages.minimal.rust-std;
        };
        rust-src = fenixPackages.stable.rust-src;
        rustfmt = fenixPackages.default.rustfmt;
        clippy = fenixPackages.default.clippy;
      in
      {
        devShells.default = pkgs.mkShell {
          inherit nativeBuildInputs;

          RUST_SRC_PATH = "${rust-src}/bin/rust-lib/src";
          LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath nativeBuildInputs;
          LIBCLANG_PATH = "${pkgs.libclang.lib}/lib";

          # Use the dyn library for local development to improve build times.
          # Name of this var is defined here https://github.com/rust-rocksdb/rust-rocksdb/blob/master/librocksdb-sys/build.rs 
          ROCKSDB_LIB_DIR = "${pkgs.rocksdb}/lib";

          NIX_LDFLAGS = "${pkgs.lib.optionalString pkgs.stdenv.isDarwin "\
            -F${pkgs.darwin.apple_sdk.frameworks.Security}/Library/Frameworks -framework Security \
            -F${pkgs.darwin.apple_sdk.frameworks.CoreFoundation}/Library/Frameworks -framework CoreFoundation"}";

          buildInputs = with pkgs; [
            (fenixPackages.combine [ cargo.stable rustc.stable rust-std.stable rust-src rustfmt ])

            (writeShellApplication {
              name = "cargo-nightly";
              runtimeInputs = [ cargo.nightly rustc.nightly rust-std.nightly clippy ];
              text = ''cargo "$@"'';
            })

            # TODO: seems to be broken currently, restore later
            # fenixPackages.rust-analyzer 
            rust-analyzer
            cargo-nextest
            cargo-udeps
            just
            docker-compose
            terraform
            ssm-session-manager-plugin
            awscli2
            jq
            jsonnet
            jsonnet-language-server
            # setting LIBCLANG_PATH manually breaks globally installed `ssh` binary and transitively breaks git
            # so we use a local version of `ssh` in this dev env (maybe there is a better way to fix it)
            # openssh
          ];

          shellHook = ''
            alias ga="$(which git) add"
            alias gst="$(which git) status"
            alias gc="$(which git) commit"
            alias gco="$(which git) checkout -b"
            alias glog="$(which git) log"
            alias lsa="ls -lah"
          '';

        };
      }
    );
}
