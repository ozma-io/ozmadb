{
  stdenv,
  dotnet-sdk_8,
  nodejs_22,
  python3,
  buildFHSUserEnv,
  writers,
}: let
  dotnet = dotnet-sdk_8;
  nodejs = nodejs_22;
  python = python3;
in
  buildFHSUserEnv {
    name = "ozmadb";
    targetPkgs = pkgs:
      with pkgs; [
        dotnet
        zlib
        mono
        lttng-ust
        libunwind
        krb5
        lldb
        openssl
        nodejs
        yarn-berry
        poetry
        python
        ruff
        pyright
        # Required for vsdbg
        icu
      ];
    extraOutputsToInstall = ["dev"];
    profile = ''
      export DOTNET_ROOT=${dotnet}
    '';
    runScript = writers.writeBash "run-script" ''
      if [ "$#" = 0 ]; then
        exec "''${SHELL:-bash}"
      else
        exec "$@"
      fi
    '';
  }
