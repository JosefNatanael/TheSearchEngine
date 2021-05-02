# Installing the Backend Search Engine in the VM

Simply run the following command to start the program. Hassle free.

```
java -jar searchengine-0.0.1-SNAPSHOT.jar
```

The Spring Boot application JAR file is generated with IntelliJ Maven plugin (which is built into the IDE). Simply run install in the Maven lifecycle.

Once we have the Spring Boot application JAR file, we can proceed to deploy it as a service in the virtual machine. For our purposes, we place the jar file searchengine-0.0.1-SNAPSHOT.jar at /home/sed/project and suppose 143.89.130.177 is the VM host url.

If the JAR file is placed anywhere else, please change the PATH_TO_JAR variable in newscript.sh accordingly.

1. Place newscript.sh in the virtual machine.
2. There are three uses to the bash script:
   - newscript.sh start starts the Spring Boot application
   - newscript.sh stop stops the Spring Boot application
   - newscript.sh restart bash restarts the Spring Boot application
3. Start the Spring Boot application with the start command above. You will be prompted to fill in the details for scraping and indexing. Output logs can be seen by opening output.txt
4. After it finishes, localhost is ready to process all your queries, as spring boot comes with a web server. However, we still need to forward our localhost to the outside world. Next, we need to open port 80, and close others in the VM. (We specified port 80 in application.properties
5. Run sudo firewall-cmd --list-all to see all allowed services, and stop all except ssh. In our case, they are dhcpv6-client and ssh. We stop dhcpv6-client

   `sudo firewall-cmd --remove-service=dhcpv6-client --permanent`

6. Next, add HTTP service or port 80 with the following command and restart:
   - `sudo firewall-cmd --add-service=http --permanent`
   - `sudo firewall-cmd --reload`
7. The endpoints 143.89.130.177/index, 143.89.130.177/metadata, etc. are served successfully to the outside world.

# Installing the Front-end Search Engine in the VM

Utilizing Apache in CentOS, the instruction are as follows:

1. First, update the packages inside the system using the following command

`sudo yum update && sudo yum upgrade`

2. Create the host directory as follows

`sudo mkdir -p /var/www/html`

3. Set permissions for the new directory to allow your regular user account to write to it

`sudo chmod 755 -R /var/www/html`

4. This user in the command below must be the owner of your site’s web root. Replace example_user with your own user’s name and /var/www/html with the location of your site’s web root.

`sudo chown -R example_user:example_user /var/www/html`

5. Set permissions for the new directory to allow your regular user account to write to it

`sudo chmod 755 -R /var/www/html`

6. Use SELinux’s chcon command to change the file security context for web content

`sudo chcon -t httpd_sys_content_t /var/www/html -R`
`sudo chcon -t httpd_sys_rw_content_t /var/www/html -R`

7. Configure the web server by editing the web server configuration in httpd.conf using the following command

`vi /etc/httpd/conf/httpd.conf`

8. Restart the web server to apply the changes

`sudo systemctl restart apache2`

9. Now, you can put your files inside /var/www/html and remember to add .htaccess file inside the directory with the content as follows

`Options -MultiViews`

`RewriteEngine On`

`RewriteCond %{REQUEST_FILENAME} !-f`

`RewriteRule ^ index.html [QSA,L]`

10. Use the following command to start or stop your Apache

`apachectl start`

`apachectl stop`

11. To check the Apache status, use the following command

`sudo systemctl status httpd`
