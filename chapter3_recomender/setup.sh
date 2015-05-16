curl -o profiledata.tar.gz http://www.iro.umontreal.ca/~lisa/datasets/profiledata_06-May-2005.tar.gz
mkdir data
mv profiledata.tar.gz data
cd data
tar -xvf profiledata.tar.gz
rm profiledata.tar.gz
mv profiledata_06-May-2005/* ./
rm -rf profiledata_06-May-2005
