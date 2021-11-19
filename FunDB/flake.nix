{
  inputs = {
    nixpkgs.url = "git+ssh://git@bitbucket.org/myprocessx/nixpkgs?ref=vabl";
    flake-utils.url = "github:numtide/flake-utils";
  };
  
  outputs = { self, flake-utils, nixpkgs }: flake-utils.lib.eachDefaultSystem (system:
    let pkgs = import nixpkgs { inherit system; };
    in {
      devShell = pkgs.callPackage ./shell.nix { };
    });
}
