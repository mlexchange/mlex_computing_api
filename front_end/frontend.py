import os
import json
import dash
from dash import Dash, html, dcc, dcc, dash_table, MATCH, ALL
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
import dash_daq as daq
import numpy as np
import pandas as pd
import pathlib
import PIL.Image as Image
import plotly.express as px
import plotly.graph_objects as go
import requests
import uuid

from assets import templates


### GLOBAL VARIABLES AND DATA LOADING
COMP_URL = 'http://job-service:8080/api/v0/'


class Constraints:
    def __init__(self, num_nodes, num_cpus, list_gpus):
        if list_gpus is None:
            list_gpus = []
        else:
            list_gpus = list_gpus.split(',')
        self.num_processors = num_cpus
        self.num_gpus = len(list_gpus)
        self.list_gpus = list_gpus
        self.num_nodes = num_nodes


class MlexHost:
    def __init__(self, nickname, hostname, front_num_workers, front_num_cpus, front_list_gpus, back_num_workers,
                 back_num_cpus, back_list_gpus):
        self.nickname = nickname
        self.hostname = hostname
        self.frontend_constraints = Constraints(front_num_workers, front_num_cpus, front_list_gpus).__dict__
        self.backend_constraints = Constraints(back_num_workers, back_num_cpus, back_list_gpus).__dict__


#### SETUP DASH APP ####
external_stylesheets = [dbc.themes.BOOTSTRAP, "../assets/segmentation-style.css"]
app = Dash(__name__, external_stylesheets=external_stylesheets)#, suppress_callback_exceptions=True)
server = app.server
app.title = "MLExchange | Computing Services"

### BEGIN DASH CODE ###
header = templates.header()

SIDEBAR = [
    dbc.Card(
        id="sidebar",
        children=[
            dbc.CardHeader("Query Compute Resources"),
            dbc.CardBody([
                dbc.InputGroup([
                    dbc.InputGroupText("Name: "),
                    dbc.Input(id="query-name", placeholder="Name", type="text")],
                    className="mb-3",
                ),
                dbc.InputGroup([
                    dbc.InputGroupText("Hostname: "),
                    dbc.Input(id="query-hostname", placeholder="Hostname", type="text")],
                    className="mb-3",
                ),
                dbc.InputGroup([
                    dbc.Button("Query",
                               outline=True,
                               color="primary",
                               id="query-host",
                               style={"text-transform": "none", "width":"100%"})
                ]),
                dbc.InputGroup([
                    dbc.Button("Submit New Host",
                               outline=True,
                               color="primary",
                               id="open-new-host",
                               style={"text-transform": "none", "width":"100%", "margin-top": "15px"})
                ])
            ])
        ]
    )
]


NEW_HOST = html.Div([
    dbc.Modal(
        id='new-host',
        is_open=False,
        centered=True,
        size="xl",
        children=[
            dbc.ModalHeader("New Host"),
            dbc.ModalBody([
                dbc.InputGroup([
                    dbc.InputGroupText("Name: "),
                    dbc.Input(id={"type":"nickname", "index":0}, placeholder="Name", type="text")],
                    className="mb-3",
                ),
                dbc.InputGroup([
                    dbc.InputGroupText("Hostname: "),
                    dbc.Input(id={"type":"hostname", "index":0}, placeholder="Hostname", type="text")],
                    className="mb-3",
                ),
                dbc.Label("Frontend Constraints:"),
                dbc.InputGroup([
                    dbc.InputGroupText("Number of workers"),
                    dbc.Input(id={"type":"front-num-workers", "index":0}, placeholder="Frontend", type="number"),
                    dbc.InputGroupText("Number of CPUs"),
                    dbc.Input(id={"type":"front-num-processors", "index":0}, placeholder="Frontend", type="number"),
                    dbc.InputGroupText("List of GPUs"),
                    dbc.Input(id={"type":"front-list-gpus", "index":0}, placeholder="Frontend", type="text")],
                    className="mb-3",
                ),
                dbc.Label("Backend Constraints:"),
                dbc.InputGroup([
                    dbc.InputGroupText("Number of workers"),
                    dbc.Input(id={"type":"back-num-workers", "index":0}, placeholder="Backend", type="number"),
                    dbc.InputGroupText("Number of CPUs"),
                    dbc.Input(id={"type":"back-num-processors", "index":0}, placeholder="Backend", type="number"),
                    dbc.InputGroupText("List of GPUs"),
                    dbc.Input(id={"type":"back-list-gpus", "index":0}, placeholder="Backend", type="text")],
                    className="mb-3",
                ),
                dbc.InputGroup([
                    dbc.InputGroupText("Public?"),
                    daq.BooleanSwitch(id={"type":'privacy', "index":0}, on=False)],
                    className="mb-3",
                ),
                dbc.InputGroup([
                    dbc.Button("Submit Computing Resource",
                               outline=True,
                               color="primary",
                               id={"type":"submit-new-host", "index":0},
                               style={"text-transform": "none", "width":"100%"})
                ])
            ])
        ])
])


MESSAGE = html.Div([
    dbc.Modal([
        dbc.ModalBody(id="msg-body"),
        dbc.ModalFooter(dbc.Button("Close", id="close-msg", className="ms-auto", n_clicks=0)),
    ],  id="modal-msg",
        centered=True,
        is_open=False,
    )
])


RESOURCES_PLOT = dbc.Card(id="plot-card",
                          children=[dbc.CardHeader("Resources Plot"),
                                    dbc.CardBody([
                                        dcc.Graph(id='resources-plot', style={'width':'100%', 'height': '20rem'})
                                    ])
                                    ]
                          )


# Resources Table
RESOURCES_TABLE = dbc.Card(
    children=[
            dbc.CardHeader("List of Computing Resources"),
            dbc.CardBody(
                children=[
                    dash_table.DataTable(
                        id='comp-table',
                        columns=[
                            {'name': 'Host ID', 'id': 'host_id'},
                            {'name': 'Name', 'id': 'nickname'},
                            {'name': 'Hostname', 'id': 'hostname'},
                            {'name': 'Total Workers', 'id': 'num_workers'},
                            {'name': 'Total CPU', 'id': 'num_cpus'},
                            {'name': 'Total GPU', 'id': 'num_gpus'}
                        ],
                        data=[],
                        hidden_columns=['host_id'],
                        row_selectable='single',
                        style_cell={'padding': '1rem',
                                    'textAlign': 'left',
                                    'overflow': 'hidden',
                                    'textOverflow': 'ellipsis',
                                    'font-size': '15px',
                                    'maxWidth': 0},
                        fixed_rows={'headers': True},
                        css=[{"selector": ".show-hide", "rule": "display: none"}],
                        style_table={'height': '30rem', 'overflowY': 'auto', 'overflowX': 'scroll'}
                    )
                ],
            )
    ]
)


MAIN_COLUMN = html.Div([
    RESOURCES_PLOT,
    RESOURCES_TABLE,
    dcc.Interval(id='interval', interval=5 * 1000, n_intervals=0)]
)


##### DEFINE LAYOUT ####
app.layout = html.Div(
    [
        header,
        dbc.Container([
                dbc.Row([
                    dbc.Col(SIDEBAR, width=4),
                    dbc.Col(MAIN_COLUMN, width=8)
                ]),
            MESSAGE,
            NEW_HOST
        ], fluid=True
        )
    ]
)

##### CALLBACKS ####
@app.callback(
    Output('comp-table', 'data'),
    Input('query-host', 'n_clicks'),
    State('query-name', 'value'),
    State('query-hostname', 'value')
)
def load_resources_list(query_n_clicks, name, hostname):
    '''
    This callback dynamically populates the list of computing hosts
    Args:
        query_n_clicks:     Button that triggers the callback
        name:               Nickname of compute resource
        hostname:           Hostname of compute resource
    Returns:
        comp_table:         List of compute resources
    '''
    host_list = requests.get(f'{COMP_URL}hosts', params={'hostname': hostname, 'nickname': name}).json()
    comp_table = []
    if host_list:
        for host in host_list:
            num_workers = host['frontend_constraints']['num_nodes'] + host['backend_constraints']['num_nodes']
            num_cpus = host['frontend_constraints']['num_processors'] + host['backend_constraints']['num_processors']
            num_gpus = host['frontend_constraints']['num_gpus'] + host['backend_constraints']['num_gpus']
            comp_table.append({'host_id': host['uid'],
                               'nickname': host['nickname'],
                               'hostname': host['hostname'],
                               'num_workers': num_workers,
                               'num_cpus': num_cpus,
                               'num_gpus': num_gpus})
    return comp_table


@app.callback(
    Output('modal-msg', 'is_open'),
    Output('new-host', 'is_open'),
    Output('msg-body', 'children'),
    Input('open-new-host', 'n_clicks'),
    Input('close-msg', 'n_clicks'),
    Input({'type':'submit-new-host', "index": ALL}, 'n_clicks'),
    State({'type':'nickname', "index": ALL}, 'value'),
    State({'type':'hostname', "index": ALL}, 'value'),
    State({'type':'privacy', "index": ALL}, 'value'),
    State({'type':'front-num-workers', "index": ALL}, 'value'),
    State({'type':'front-num-processors', "index": ALL}, 'value'),
    State({'type':'front-list-gpus', "index": ALL}, 'value'),
    State({'type':'back-num-workers', "index": ALL}, 'value'),
    State({'type':'back-num-processors', "index": ALL}, 'value'),
    State({'type':'back-list-gpus', "index": ALL}, 'value'),
    prevent_initial_call=True
)
def submit_new_host(open_new_host, close_msg, submit_new_host, nickname, hostname, privacy, front_num_workers, front_num_cpus,
                    front_list_gpus, back_num_workers, back_num_cpus, back_list_gpus):
    '''
    This callback submits a new host to the compute API
    Args:
        open_new_host:          Open form to submit new host
        close_msg:              Button to close modal with message
        submit_new_host:        Submit new host
        nickname:               [str] Host nickname
        hostname:               [str] Hostname
        front_num_workers:      [int] Number of workers for frontend services
        front_num_cpus:         [int] Number of processors for frontend services
        front_list_gpus:        [str] List of GPUs for frontend services
        back_num_workers:       [int] Number of workers for backend services
        back_num_cpus:          [int] Number of processors for backend services
        back_list_gpus:         [str] List of GPUs for backend services
        privacy:                [bool] public or not public
    Return:
        open_msg:               Open confirmation message
        open_host_form:         Open new host form
        msg_body:               Body of confirmation message
    '''
    changed_id = [p['prop_id'] for p in dash.callback_context.triggered][0]
    open_msg = dash.no_update
    open_host_form = dash.no_update
    msg_body = dash.no_update
    if 'close-msg.n_clicks' in changed_id:
        open_msg = False
    if 'open-new-host.n_clicks' in changed_id:
        open_host_form = True
    if '{"index":0,"type":"submit-new-host"}.n_clicks' in changed_id:
        mlex_host = MlexHost(nickname[0], hostname[0], front_num_workers[0], front_num_cpus[0],
                             front_list_gpus[0], back_num_workers[0], back_num_cpus[0],
                             back_list_gpus[0])
        response = requests.post(f'{COMP_URL}hosts', json=mlex_host.__dict__)
        if response.status_code == 200:
            msg_body = 'The host has been sucessfully submitted!'
        else:
            msg_body = f'The host was not submitted. Error code: {response.status_code}'
        open_msg = True
        open_host_form = False
    return open_msg, open_host_form, msg_body


@app.callback(
    Output('resources-plot', 'figure'),
    Input('comp-table', 'selected_rows'),
    Input('interval', 'n_intervals'),
    State('resources-plot', 'figure'),
    State('comp-table', 'data'),
)
def plot_resources(row, time, current_fig, comp_table):
    '''
    This callback plots the current number of resources utilized at the host location
    Args:
        row:                    Selected row in list
        time:                   Time
        current_fig:            Current resources plot
        comp_table:             Data in table
    Returns:
        resources_plot:         Plot of resources
    '''
    fig = go.Figure(go.Scatter(x=[], y=[]))
    fig.update_layout(margin=dict(l=20, r=160, t=20, b=20))
    changed_id = [p['prop_id'] for p in dash.callback_context.triggered][0]
    if row:
        host_id = comp_table[row[0]]["host_id"]
        host = requests.get(f'{COMP_URL}hosts/{host_id}').json()
        num_processors = host['frontend_constraints']['num_processors'] + host['backend_constraints']['num_processors'] - \
                         host['backend_available']['num_processors'] - host['frontend_available']['num_processors']
        num_gpus = host['frontend_constraints']['num_gpus'] + host['backend_constraints']['num_gpus'] - \
                   host['backend_available']['num_gpus'] - host['frontend_available']['num_gpus']
        resources = {}
        if len(current_fig['data'][0]['x']) > 0 and 'comp-table.selected_rows' not in changed_id:
            resources['time'] = np.append(current_fig['data'][0]['x'], current_fig['data'][0]['x'][-1]+1)
            resources['num_processors'] = np.append(current_fig['data'][0]['y'], num_processors)
            resources['num_gpus'] = np.append(current_fig['data'][1]['y'], num_gpus)
        else:
            resources['time'] = [0]
            resources['num_processors'] = [num_processors]
            resources['num_gpus'] = [num_gpus]
        df = pd.DataFrame.from_dict(resources)
        df.set_index('time', inplace=True)
        try:
            fig = px.line(df, markers=True)
            fig.update_layout(margin=dict(l=20, r=20, t=20, b=20))
        except Exception as e:
            print(e)
    fig.update_layout(xaxis_title="time[s]", yaxis_title="resources")
    return fig


if __name__ == "__main__":
    app.run_server(debug=True, host='0.0.0.0', port=8050)
