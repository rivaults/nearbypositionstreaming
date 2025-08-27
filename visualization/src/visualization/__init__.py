from typing import Tuple

from dash_extensions.enrich import DashProxy
from flask import Flask


def create_app() -> Tuple[Flask, DashProxy]:
    server = Flask(__name__)
    app = DashProxy(
        __name__,
        server=server,
        suppress_callback_exceptions=True,
        title='Nearby',
        external_stylesheets=[],
        external_scripts=[
            {'src': "static/utils.mjs", "type": "module"},
        ]
    )
    return server, app
