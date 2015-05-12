curl https://archive.ics.uci.edu/ml/machine-learning-databases/00210/donation.zip -o donation.zip
unzip donation.zip
mkdir data
mv block* ./data
cd ./data
unzip 'block*.zip'
rm *.zip
cd ..
rm donation.zip
