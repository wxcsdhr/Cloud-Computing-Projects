export TEAMID="Craig300"
export AWSACCOUNT="057914033338"
export DBUSER="root"
export DBPWD="craig300"
sudo iptables -A PREROUTING -t nat -p tcp --dport 80 -j REDIRECT --to-port 8080
mvn clean package assembly:single && java -jar -Xmx4096m target/TeamServer-1.0-jar-with-dependencies.jar
