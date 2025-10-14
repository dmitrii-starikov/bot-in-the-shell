### Install local Elasticsearch (use VPN for download) ###
wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-8.5.3-amd64.deb
dpkg -i elasticsearch-8.5.3-amd64.deb
systemctl daemon-reload
systemctl enable elasticsearch.service
mv /etc/elasticsearch/elasticsearch.yml /etc/elasticsearch/elasticsearch_def.yml
cp -Rf ../configs/elasticsearch.yml /etc/elasticsearch/elasticsearch.yml
mv /etc/elasticsearch/jvm.options /etc/elasticsearch/jvm.options_def
cp -Rf ../configs/jvm.options /etc/elasticsearch/jvm.options
iptables -I INPUT -p tcp -m tcp --dport 9200 -j ACCEPT
# apt install net-tools
# netstat -tulpn
apt install openjdk-17-jre-headless
sudo -u elasticsearch -s /usr/share/elasticsearch/bin/elasticsearch-keystore create
chmod g+w /etc/elasticsearch
chown -R elasticsearch:elasticsearch /etc/elasticsearch/
systemctl restart elasticsearch.service
# apt  install curl
# curl -XGET http://127.0.0.1:9200
rm elasticsearch-8.5.3-amd64.deb