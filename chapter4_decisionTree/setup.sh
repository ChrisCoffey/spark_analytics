rm -rf ./data
mkdir data
cd data
curl -o covtype.data.gz https://archive.ics.uci.edu/ml/machine-learning-databases/covtype/covtype.data.gz
curl -o covtype.info https://archive.ics.uci.edu/ml/machine-learning-databases/covtype/covtype.info
gzip -d covtype.data.gz
mv covtype.data ./covtype.data.csv
