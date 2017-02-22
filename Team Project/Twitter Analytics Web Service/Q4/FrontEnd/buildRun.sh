export TEAM_ID="Craig300"
export TEAM_AWS_ACCOUNT_ID="057914033338"
export DBUSER="root"
export DBPWD="craig300"
sudo iptables -A PREROUTING -t nat -p tcp --dport 80 -j REDIRECT --to-port 8080
java -jar -Xmx4096m TeamServer-1.0-jar-with-dependencies.jar
