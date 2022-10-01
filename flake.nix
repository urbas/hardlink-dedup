{
  description = "A prototype for file deduplication with hard links.";

  inputs.nixpkgs.url = "nixpkgs/nixpkgs-unstable";

  outputs = { self, nixpkgs }:
    let
      forAllSystems = f: nixpkgs.lib.genAttrs [ "x86_64-linux" "aarch64-linux" ] (system: f { inherit system; pkgs = import nixpkgs { inherit system; }; });

      packageName = "hardlink-dedup";

    in {
      packages = forAllSystems ({pkgs, ... }: with pkgs;
        let
          binDeps = [
            coreutils
            diffutils
            fd
          ];

          checkBinDeps = [
            rsync
          ];

          pyDevDeps = with python3Packages; [
            black
            flake8
            mypy
          ];

          propagatedBuildInputs = with python3Packages; [
            click
          ];

          checkInputs = with python3Packages; [
            pytestCheckHook
            pytest-watch
          ];

        in {
          default = with python3Packages; buildPythonPackage {
            name = packageName;
            src = ./.;
            nativeBuildInputs = pyDevDeps;
            propagatedBuildInputs = propagatedBuildInputs ++ binDeps;
            checkInputs = checkInputs ++ binDeps ++ checkBinDeps;
          };

          devEnv = buildEnv {
            name = "devEnv";
            paths = binDeps ++ checkBinDeps ++ [(python3.withPackages(_: pyDevDeps ++ propagatedBuildInputs ++ checkInputs))];
          };
        }
      );
    };
}
