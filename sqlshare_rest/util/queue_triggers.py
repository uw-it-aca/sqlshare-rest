import socket

SNAPSHOT_QUEUE_PORT_NUMBER = 1997
UPLOAD_QUEUE_PORT_NUMBER = 1998
QUERY_QUEUE_PORT_NUMBER = 1999


def trigger_query_queue_processing():
    try:
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(('localhost', QUERY_QUEUE_PORT_NUMBER))
        return True
    except socket.error as ex:
        return False


def trigger_upload_queue_processing():
    try:
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(('localhost', UPLOAD_QUEUE_PORT_NUMBER))
        return True
    except socket.error as ex:
        return False


def trigger_snapshot_processing():
    try:
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(('localhost', SNAPSHOT_QUEUE_PORT_NUMBER))
        return True
    except socket.error as ex:
        return False
