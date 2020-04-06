cd /MayaStor
nix-shell --run 'pushd mayastor && while [ $? -eq 0 ]; do cargo test -- --test-threads=1; done;'
