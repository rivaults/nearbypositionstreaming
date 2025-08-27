import dash_leaflet as dl
from dash import (
    html,
    Output,
    Input,
    State,
    dcc,
    clientside_callback,
    ClientsideFunction,
    Dash
)
from dash_extensions import WebSocket

from . import create_app
from .config import CONNECT_HOST, LISTEN_HOST, PHONE_PATTERN

server, app = create_app()

app.layout = html.Div([
    dcc.Store(id='store-state', data={"state_id": 0}),
    dcc.Dropdown(list(range(182)), 0, id='choose-id'),
    dl.Map(
        [
            dl.TileLayer(),
            dl.PolylineDecorator(
                id="phone",
                positions=[],
                patterns=PHONE_PATTERN
            ),
            dl.LayerGroup(
                [],
                id="nearby"
            )
        ],
        id="map",
        center=[56, 10],
        zoom=18,
        style={"height": "100vh"}
    ),
    WebSocket(
        id="ws-data",
        url=f"ws://{CONNECT_HOST}:5000/data",
        timeout=60000
    )
])


clientside_callback(
    ClientsideFunction(
        namespace='clientside',
        function_name='ws_send'
    ),
    Output("ws-data", "send"),
    Input("choose-id", "value"),
    State("store-state", "data"),
    prevent_initial_call=True
)

clientside_callback(
    ClientsideFunction(
        namespace='clientside',
        function_name='ws_receive'
    ),
    Output("nearby", "children"),
    Input("ws-data", "message"),
    State("phone", "positions"),
    State("nearby", "children"),
    State("nearby", "orders"),
    prevent_initial_call=True
)

def main() -> None:
    app.run(LISTEN_HOST, 8050, debug=True)