#!/usr/bin/env python3
import os
import shlex
import subprocess
import tempfile
import unittest

TOPIC = "kafka-cli-test"
BROKER = "127.0.0.1:9092"


class TestKafkaCli(unittest.TestCase):

    def test_produce(self):
        c = subprocess.Popen(shlex.split(f"./kafka-cli --brokers={BROKER} consume {TOPIC}"),
                stdout=subprocess.PIPE)
        try:
            with tempfile.NamedTemporaryFile(prefix="kafka-cli", delete=False) as f:
                f.write(b"TEST")
                f.flush()
                fn = f.name
                subprocess.check_output(shlex.split(f"./kafka-cli --brokers={BROKER} produce {TOPIC} {fn} key"))
            stdout = c.stdout
            data = stdout.readline()
            stdout.close()
            self.assertRegex(str(data),
                r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+\d{2}:\d{2} "+TOPIC+"\(\d+:\d+\) TEST")
        finally:
            c.kill()
            c.wait()


if __name__ == '__main__':
    env = os.environ
    env["KAFKA_CLI_USER"] = "admin"
    env["KAFKA_CLI_PASSWORD"] = "admin-secret"
    unittest.main()
