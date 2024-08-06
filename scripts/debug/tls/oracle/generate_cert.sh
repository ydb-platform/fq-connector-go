#!/bin/bash

certsDir=./

function createDirIfNotExists {
    dirName=$1
    if [ ! -d $dirName ]; then 
        echo "creating dir '$dirName'"
        mkdir -p $dirName
    fi
}

function setCertsPermissions {
    sudo chmod +r client.key
}

function createCerts {
    createDirIfNotExists $certsDir
    cd $certsDir

    echo "Generate RSA client.key 4096"

    openssl genrsa -out client.key 4096

    echo "Generate client.csr"

    openssl req -new -key client.key -out client.csr -subj '/CN=oracle' -addext "subjectAltName = DNS:oracle"

    echo "Generate RSA caCert.key 4096"

    openssl genrsa -out caCert.key 4096

    echo "Generate caCert.crt"

    openssl req -new -x509 -days 1826 -key caCert.key -out caCert.crt -subj '/CN=oracle/C=US/OU=Class 2 Public Primary Certification Authority/O=VeriSign' -addext "subjectAltName = DNS:oracle"

    echo "Generate cert.crt subscribing by caCert from client.csr"

    openssl x509 -req -days 730 -in client.csr -CA caCert.crt -CAkey caCert.key -extensions v3_ca -extfile ./extfile.cnf -set_serial 01  -out cert.crt

    setCertsPermissions

    cd ..
}



preRunDir=($PWD)
scriptDir=$0
cd $0

createCerts

cd $preRunDir