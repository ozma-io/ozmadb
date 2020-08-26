{ nixpkgs ? import <nixpkgs> {} }@args:

(import ../common.nix args).fhsShell.override {
  extraPkgs = pkgs: with pkgs; [ dotnet-sdk_3 ];
  extraLibraries = pkgs: with pkgs; [ zlib icu lttng-ust libunwind kerberos ];
}
