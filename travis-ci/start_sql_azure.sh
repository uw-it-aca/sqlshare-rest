
curl -X POST https://management.core.windows.net:8443/$SUBSCRIPTION_ID/services/sqlservers/servers -d "<Server xmlns='http://schemas.microsoft.com/sqlazure/2010/12/'><AdministratorLogin>sqlshare-travis-admin</AdministratorLogin><AdministratorLoginPassword>$AZURE_DB_PASSWORD</AdministratorLoginPassword><Location>West US</Location><Version>12.0</Version></Server>" --key /tmp/azure/azure.key --cert /tmp/azure/azure.cert --header 'x-ms-version: 2012-03-01' --header 'Content-type: application/xml' > /tmp/azure/server_info.xml

cat /tmp/azure/server_info.xml
export SERVER_HOST=`xmllint --xpath '/*[local-name()="ServerName"]/@FullyQualifiedDomainName' /tmp/azure/server_info.xml | cut -d '"' -f 2`
export SERVER_NAME=`xmllint --xpath '/*[local-name()="ServerName"]/text()' /tmp/azure/server_info.xml`

echo "Azure DB Name: $SERVER_NAME"

IP=`wget http://ipecho.net/plain -O - -q ; echo`

echo "Public IP: $IP"
curl -X POST https://management.core.windows.net:8443/$SUBSCRIPTION_ID/services/sqlservers/servers/$SERVER_NAME/databases -d '<ServiceResource xmlns="http://schemas.microsoft.com/windowsazure"><Name>sqlshare</Name></ServiceResource>' --key /tmp/azure/azure.key --cert /tmp/azure/azure.cert --header "x-ms-version: 2012-03-01" --header "Content-type: application/xml" > database_info.xml

curl -X POST https://management.core.windows.net:8443/$SUBSCRIPTION_ID/services/sqlservers/servers/$SERVER_NAME/firewallrules -d "<ServiceResource xmlns='http://schemas.microsoft.com/windowsazure'><Name>AllowAll</Name><StartIPAddress>$IP</StartIPAddress><EndIPAddress>$IP</EndIPAddress></ServiceResource>" --key /tmp/azure/azure.key --cert /tmp/azure/azure.cert --header "x-ms-version: 2012-03-01" --header "Content-type: application/xml"

printf "[FreeTDS]\nDescription = Azure DB\nDriver = /usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so\nSetup = /usr/lib/x86_64-linux-gnu/odbc/libtdsS.so\nTDS_Version = 7.0\nDataTypeCompatibility=80\nMARS Connection=False\n" > /tmp/azure/odbcinst.ini
printf "[Azure]\nSERVER = $SERVER_HOST\nDriver = FreeTDS\n" > /tmp/azure/odbc.ini

export ODBCSYSINI=/tmp/azure/
export ODBCINI=/tmp/azure/odbc.ini

