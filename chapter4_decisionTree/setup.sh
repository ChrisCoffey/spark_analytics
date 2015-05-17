rm -rf ./data
mkdir data
cd data
curl -o covtype.data.gz https://archive.ics.uci.edu/ml/machine-learning-databases/covtype/covtype.data.gz
gzip -d covtype.data.gz
mv covtype.data ./covtype.data.csv
