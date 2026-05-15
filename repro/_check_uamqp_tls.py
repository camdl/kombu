"""Smoke test: bare azure-servicebus client with uamqp_transport=True
connecting to the emulator THROUGH stunnel (TLS on 5671 -> plain AMQP
on 5672).

If this passes, the same wiring can be reused under gevent.
"""
import logging

from azure.servicebus import (
    ServiceBusClient, ServiceBusMessage, ServiceBusReceiveMode,
)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")
logging.getLogger("uamqp").setLevel(logging.WARNING)

CS = (
    "Endpoint=sb://localhost:5671;"
    "SharedAccessKeyName=RootManageSharedAccessKey;"
    "SharedAccessKey=SAS_KEY_VALUE;"
)
CA = "/workspaces/kombu/repro/tls/cert.pem"


def main():
    client = ServiceBusClient.from_connection_string(
        CS,
        uamqp_transport=True,
        connection_verify=CA,
        retry_total=1,
    )
    print(f"client transport: {type(client._amqp_transport).__name__}")

    with client:
        with client.get_queue_receiver(
            "test-renewal",
            receive_mode=ServiceBusReceiveMode.RECEIVE_AND_DELETE,
            max_wait_time=2,
        ) as r:
            n = 0
            while True:
                msgs = r.receive_messages(
                    max_message_count=10, max_wait_time=1)
                if not msgs:
                    break
                n += len(msgs)
            print(f"drained {n} stale messages")

        with client.get_queue_sender("test-renewal") as s:
            s.send_messages(ServiceBusMessage("uamqp-tls-smoke"))
        print("sent")

        with client.get_queue_receiver(
                "test-renewal", max_wait_time=10) as r:
            msgs = r.receive_messages(
                max_message_count=1, max_wait_time=10)
            print(f"received {len(msgs)} message(s)")
            if msgs:
                m = msgs[0]
                body = m.body if isinstance(m.body, bytes) else b''.join(
                    m.body)
                print(f"body: {body.decode()}")
                print(f"locked_until_utc: {m.locked_until_utc}")
                r.complete_message(m)
                print("complete: ok")


if __name__ == "__main__":
    main()
