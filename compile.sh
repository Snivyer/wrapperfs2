if [ ! -d build ]; then
    mkdir -p build
fi
cd build
cmake .. && cmake --build . -j8


cd ..
./mkfs.sh

