docker cp C:\Users\admin\java\src\compare-processor\nifi-compare-nar\target\%1 nifi:/opt/nifi/nifi-current/extensions
docker exec nifi bash /opt/nifi/nifi-current/bin/nifi.sh restart