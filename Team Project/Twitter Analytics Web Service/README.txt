Cloud Computing Team Project
Q1 - Phase1 Q1 (Front End)
Q2 - Phase1 Q2 (ETL)
Q3 - Phase2 Q1 
...

We use Maven to manage each project.
Each project folder contains a build.sh, change it if necessary.

Redirect port 80 t0 port 8080
sudo iptables -A PREROUTING -t nat -p tcp --dport 80 -j REDIRECT --to-port 8080


Git instruction:
1. mkdir xxx && cd xxx
2. git clone https://f16cc@bitbucket.org/f16cc/teamserver.git
3. start working
	--check source folder change: git status

	--stage code change: git add xxx or git add -A

	--commit change to local repository: git commit -m 'commit message'

	--push to remote repository:
		1. git remote add origin https://f16cc@bitbucket.org/f16cc/teamserver.git
		2. git push origin master

Attention:
1. Remember to pull latest code to local repository before making any change!
	--git pull origin master

2. Remember to build & run before committing.

3. Remember to commit changes frequently so that everyone can work on the latest code.

4. Recover previous version:
	--git log (check commit history, find out the commit id)
	--git checkout commit_id (attention: all the uncommitted local change will be lost!)
