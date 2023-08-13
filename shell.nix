{ pkgs ? import <nixpkgs> {} }:

let
  dotnet = pkgs.dotnet-sdk_7;

  env = pkgs.buildFHSUserEnv {
    name = "fundb";
    targetPkgs = pkgs: with pkgs; [
      dotnet
      zlib
      mono
      gcc
      gnumake
      icu
      lttng-ust
      libunwind
      kerberos
      lldb
      openssl
    ];
    extraOutputsToInstall = [ "dev" ];
    profile = ''
      export DOTNET_ROOT=${dotnet}
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

