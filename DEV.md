## How to run test

1. Download kafka from the official website. (The zookeeper is shipped by the package)
2. Run zookeeper.
3. Run kafka with the server.properties: `bin/kafka-server-start.sh path/to/kafka-cli/server.properties`.
4. Run test.py (required python3): `./test.py`.
