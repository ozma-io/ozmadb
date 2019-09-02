{ nixpkgs ? import <nixpkgs> {} }@args:

(import ../common.nix args).fhsShell.override {
  extraPkgs = pkgs: with pkgs; [ dotnet-sdk ];
  extraLibraries = pkgs: with pkgs; [ icu lttng-ust libunwind ];
}
