{ pkgs ? import <nixpkgs> {} }:

let
  env = pkgs.buildFHSUserEnv {
    name = "fundb";
    targetPkgs = pkgs: with pkgs; [
      dotnet-sdk_6
      zlib
      mono
      gcc
      gnumake
      icu
      lttng-ust
      libunwind
      kerberos
    ];
    extraOutputsToInstall = [ "dev" ];
    profile = ''
      export DOTNET_ROOT=${pkgs.dotnet-sdk_6}
    '';
    runScript = pkgs.writeScript "env-shell" ''
      #!${pkgs.stdenv.shell}
      exec ${userShell}
    '';
  };

  userShell = builtins.getEnv "SHELL";

in pkgs.stdenv.mkDerivation {
  name = "fundb-fhs-dev";

  shellHook = ''
    exec ${env}/bin/fundb
  '';
  buildCommand = "exit 1";
}

