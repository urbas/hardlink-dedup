{
  description = "A prototype for file deduplication with hard links.";

  inputs.nixpkgs.url = "nixpkgs/nixpkgs-unstable";

  outputs = { self, nixpkgs }:
    let
      forAllSystems = f: nixpkgs.lib.genAttrs [ "x86_64-linux" "aarch64-linux" ] (system: f { inherit system; pkgs = import nixpkgs { inherit system; }; });

    in {
      devShells = forAllSystems ({pkgs, ...}: with pkgs; {
        default = stdenv.mkDerivation {
          name = "devEnv";
          buildInputs = [
            rustup
          ];
        };
      });
    };
}
