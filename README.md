sudo dnf install libssh2-devel
sudo dnf install libidn2-devel openldap-devel
sudo dnf install apache-kafka
sudo dnf install openssl1.1 

The library librdkafka++ is it too old, that's why we must install a new source
git clone https://github.com/confluentinc/librdkafka.git

Build from source 
Requirements 
The GNU toolchain
GNU make
pthreads
zlib-dev (optional, for gzip compression support)
libssl-dev (optional, for SSL and SASL SCRAM support)
libsasl2-dev (optional, for SASL GSSAPI support)
libzstd-dev (optional, for ZStd compression support)
libcurl-dev (optional, for SASL OAUTHBEARER OIDC support)

NOTE: Static linking of ZStd (requires zstd >= 1.2.1) in the producer enables encoding the original size in the compression frame header, which will
speed up the consumer. Use STATIC_LIB_libzstd=/path/to/libzstd.a ./configure --enable-static to enable static ZStd linking.
MacOSX example: STATIC_LIB_libzstd=$(brew ls -v zstd | grep libzstd.a$) ./configure --enable-static 
Building 
./configure
# Or, to automatically install dependencies using the system's package manager:
# ./configure --install-deps
# Or, build dependencies from source:
# ./configure --install-deps --source-deps-only
  make
sudo make install

