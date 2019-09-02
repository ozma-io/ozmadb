{ nixpkgs ? import <nixpkgs> {} }:

nixpkgs.callPackage ../common.nix {
  extraPkgs = pkgs: with pkgs; [ pkgs.dotnet-sdk ];
  extraLibraries = pkgs: with pkgs; [ pkgs.icu pkgs.lttng-ust pkgs.libunwind ];
}
