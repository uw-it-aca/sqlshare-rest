import socket

QUERY_QUEUE_PORT_NUMBER = 1999


def trigger_query_queue_processing():
    try:
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(('localhost', QUERY_QUEUE_PORT_NUMBER))
    except ConnectionRefusedError:
        pass
