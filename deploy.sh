cd Servlets 
ant clean 
ant dist 
rm -R ~/apache-tomcat-8.0.33/webapps/rubis_servlets/*
rmdir ~/apache-tomcat-8.0.33/webapps/rubis_servlets/
rm ~/apache-tomcat-8.0.33/webapps/rubis_servlets.war
rm ~/apache-tomcat-8.0.33/logs/*
cp -v rubis_servlets.war ~/apache-tomcat-8.0.33/webapps/
cd ..
