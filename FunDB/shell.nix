{ nixpkgs ? import <nixpkgs> {} }@args:

(import ../common.nix args).shell.override {
  extraPkgs = pkgs: with pkgs; [ pkgs.dotnet-sdk ];
  extraLibraries = pkgs: with pkgs; [ pkgs.icu pkgs.lttng-ust pkgs.libunwind ];
}
