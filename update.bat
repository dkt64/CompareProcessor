docker cp C:\Users\admin\java\src\compare-processor\nifi-compare-nar\target\nifi-compare-nar-1.0.nar sandbox-hdf:/usr/hdf/current/nifi/lib
docker exec sandbox-hdf bash /usr/hdf/current/nifi/bin/nifi.sh restart
