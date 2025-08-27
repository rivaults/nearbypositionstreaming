from pinot_connect import Connection, ClientOptions, connect


def create_db() -> Connection:
    return connect(
        host="pinot-broker",
        client_options=ClientOptions(timeout=600)
    )