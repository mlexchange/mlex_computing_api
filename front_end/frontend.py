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
                    dbc.Input(id="query-name", placeholder="local", type="text")],
                    className="mb-3",
                ),
                dbc.InputGroup([
                    dbc.InputGroupText("Hostname: "),
                    dbc.Input(id="query-hostname", placeholder="local.als.lbl.gov", type="text")],
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
                    dbc.Input(id={"type":"nickname", "index":0}, value="local", type="text")],
                    className="mb-3",
                ),
                dbc.InputGroup([
                    dbc.InputGroupText("Hostname: "),
                    dbc.Input(id={"type":"hostname", "index":0}, value="local.als.lbl.gov", type="text")],
                    className="mb-3",
                ),
                dbc.Label("Frontend Constraints:"),
                dbc.InputGroup([
                    dbc.InputGroupText("Number of workers"),
                    dbc.Input(id={"type":"front-num-workers", "index":0}, value=2, type="number"),
                    dbc.InputGroupText("Number of CPUs"),
                    dbc.Input(id={"type":"front-num-processors", "index":0}, value=10, type="number"),
                    dbc.InputGroupText("List of GPUs"),
                    dbc.Input(id={"type":"front-list-gpus", "index":0}, value="[]", type="text")],
                    className="mb-3",
                ),
                dbc.Label("Backend Constraints:"),
                dbc.InputGroup([
                    dbc.InputGroupText("Number of workers"),
                    dbc.Input(id={"type":"back-num-workers", "index":0}, value=2, type="number"),
                    dbc.InputGroupText("Number of CPUs"),
                    dbc.Input(id={"type":"back-num-processors", "index":0}, value=10, type="number"),
                    dbc.InputGroupText("List of GPUs"),
                    dbc.Input(id={"type":"back-list-gpus", "index":0}, value="[]", type="text")],
                    className="mb-3",
                ),
                dbc.InputGroup([
                    dbc.InputGroupText("Privacy:"),
                    daq.BooleanSwitch(id={"type":'privacy', "index":0}, on=False),
                    html.Div(id="privacy-toggle")
                ],
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
                    dbc.Row(
                        [dbc.Col(dbc.Button("RESET DATABASE",
                                            outline=True,
                                            color="danger",
                                            id="reset-database",
                                            style={"text-transform": "none", "width":"100%"})),
                         dbc.Col(dbc.Button("RESET HOST",
                                            outline=True,
                                            color="danger",
                                            id="reset-host",
                                            style={"text-transform": "none", "width":"100%"})),
                         dbc.Col(dbc.Button("DELETE HOST",
                                            outline=True,
                                            color="danger",
                                            id="delete-host",
                                            disabled=True,
                                            style={"text-transform": "none", "width":"100%"}))],
                        style={'margin-bottom': '1rem'}
                    ),
                    dash_table.DataTable(
                        id='comp-table',
                        columns=[
                            {'name': 'Host ID', 'id': 'host_id'},
                            {'name': 'Name', 'id': 'nickname'},
                            {'name': 'Hostname', 'id': 'hostname'},
                            {'name': 'Total Workers', 'id': 'num_workers'},
                            {'name': 'Total CPU', 'id': 'num_cpus'},
                            {'name': 'Total GPU', 'id': 'num_gpus'},
                            {'name': 'CPU usage', 'id': 'cpu_usage'},
                            {'name': 'GPU usage', 'id': 'gpu_usage'}
                        ],
                        data=[],
                        hidden_columns=['host_id', 'cpu_usage', 'gpu_usage'],
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


WARNING_MSG = html.Div([
    dcc.ConfirmDialog(id="warning-body")
])


MAIN_COLUMN = html.Div([
    RESOURCES_PLOT,
    RESOURCES_TABLE,
    dcc.Interval(id='interval', interval=5 * 1000, n_intervals=0),
    WARNING_MSG,
    html.Div(id='no-display',
             children=[dcc.Store(id='action', data=-1)]),
])


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
        ),
        dcc.Store(id="host-store", data=[])
    ]
)

##### CALLBACKS ####
@app.callback(
    Output("privacy-toggle", "children"),
    Input({'type':'privacy', "index": ALL}, "on")
)
def privacy_toggle(privacy):
    if privacy[0]:
        return "Public"
    else:
        return "Private"


@app.callback(
    Output("comp-table", "data"),
    Output("host-store", "data"),
    Input("query-host", "n_clicks"),
    Input("close-msg", "n_clicks"),
    Input('interval', 'n_intervals'),
    State("query-name", "value"),
    State("query-hostname", "value"),
    State("host-store", "data")
)
def load_resources_list(query_n_clicks, close_msg, time_interval, nickname_query, hostname_query, host_store):
    '''
    This callback dynamically populates the list of computing hosts
    Args:
        query_n_clicks:     [int] Button that triggers the callback
        close_msg:          [int] Button that closes pop-up messages (when updating/deleting hosts of database)
        time_interval:      [int] time
        nickname_query:     [str] Nickname of compute resource
        hostname_query:     [str] Hostname of compute resource
    Returns:
        comp_table:         [List] List of compute resources
    '''
    changed_id = [p['prop_id'] for p in dash.callback_context.triggered][0]

    if "query-host" in changed_id:
        host_list = requests.get(f'{COMP_URL}hosts', params={'hostname': hostname_query,
                                                             'nickname': nickname_query}).json()
    else:
        host_list = requests.get(f'{COMP_URL}hosts').json()

    if host_store != host_list:
        comp_table = []
        host_store = host_list
        if host_list:
            for host in host_list:
                num_workers = host['frontend_constraints']['num_nodes'] + host['backend_constraints']['num_nodes']
                num_cpus = host['frontend_constraints']['num_processors'] + host['backend_constraints']['num_processors']
                num_gpus = host['frontend_constraints']['num_gpus'] + host['backend_constraints']['num_gpus']
                cpu_usage = host['frontend_constraints']['num_processors'] + host['backend_constraints']['num_processors']\
                            - host['backend_available']['num_processors'] - host['frontend_available']['num_processors']
                gpu_usage = host['frontend_constraints']['num_gpus'] + host['backend_constraints']['num_gpus'] - \
                            host['backend_available']['num_gpus'] - host['frontend_available']['num_gpus']
                comp_table.append({'host_id': host['uid'],
                                   'nickname': host['nickname'],
                                   'hostname': host['hostname'],
                                   'num_workers': num_workers,
                                   'num_cpus': num_cpus,
                                   'num_gpus': num_gpus,
                                   'cpu_usage': cpu_usage,
                                   'gpu_usage': gpu_usage})
    else:
        comp_table = dash.no_update
        host_store = dash.no_update
    return comp_table, host_store


@app.callback(
    Output('resources-plot', 'figure'),
    Input('comp-table', 'selected_rows'),
    Input('interval', 'n_intervals'),
    Input("comp-table", "data"),
    State('resources-plot', 'figure'),
    prevent_initial_call=True
)
def plot_resources(row, time, comp_table, current_fig):
    '''
    This callback plots the current number of resources utilized at the host location
    Args:
        row:                    [List] Selected row in list
        time:                   [int] Time
        comp_table:             [List] Data in table
        current_fig:            [fig] Current resources plot
    Returns:
        resources_plot:         [fig] Plot of resources
    '''
    fig = go.Figure(go.Scatter(x=[], y=[]))
    fig.update_layout(margin=dict(l=20, r=160, t=20, b=20))
    changed_id = [p['prop_id'] for p in dash.callback_context.triggered][0]
    if row:
        if row[0]<len(comp_table):
            host = comp_table[row[0]]
            host_id = host["host_id"]
            num_processors = host["cpu_usage"]
            num_gpus = host["gpu_usage"]
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


@app.callback(
    Output("warning-body", "message"),
    Output("warning-body", "displayed"),
    Output("msg-body", "children"),
    Output("modal-msg", "is_open"),
    Output('new-host', 'is_open'),
    Output("action", "data"),
    Output({'type':'nickname', "index": ALL}, 'value'),
    Output({'type':'hostname', "index": ALL}, 'value'),

    Input("reset-database", "n_clicks"),
    Input("reset-host", "n_clicks"),
    Input("delete-host", "n_clicks"),
    Input("comp-table", "selected_rows"),
    Input("warning-body", "cancel_n_clicks"),
    Input("warning-body", "submit_n_clicks"),
    Input("close-msg", "n_clicks"),
    Input('open-new-host', 'n_clicks'),
    Input({'type':'submit-new-host', "index": ALL}, 'n_clicks'),

    State("comp-table", "data"),
    State("action", "data"),
    State({'type':'nickname', "index": ALL}, 'value'),
    State({'type':'hostname', "index": ALL}, 'value'),
    State({'type':'privacy', "index": ALL}, 'on'),
    State({'type':'front-num-workers', "index": ALL}, 'value'),
    State({'type':'front-num-processors', "index": ALL}, 'value'),
    State({'type':'front-list-gpus', "index": ALL}, 'value'),
    State({'type':'back-num-workers', "index": ALL}, 'value'),
    State({'type':'back-num-processors', "index": ALL}, 'value'),
    State({'type':'back-list-gpus', "index": ALL}, 'value'),
    State("query-name", "value"),
    State("query-hostname", "value"),
    prevent_initial_call=True
)
def show_messages(reset_database_click, reset_host_click, delete_host_click, row, cancel_del_click, confirm_del_click,
                  close_error_click, open_new_host, submit_new_host, comp_table, action, nickname, hostname,
                  privacy, front_num_workers, front_num_cpus, front_list_gpus, back_num_workers, back_num_cpus,
                  back_list_gpus, query_name, query_hostname):
    '''
    This callback managers pop-up messages
    Args:
        reset_database_click:   [int] Button "reset databaset" has been selected
        reset_host_click:       [int] Button "reset host" has been selected
        delete_host_click:      [int] Button "delete host" has been selected
        row:                    [List] Selected row in comp_table
        cancel_del_click:       [int] Button "cancel" has been selected in confirmation window
        confirm_del_click:      [int] Button "confirm" has been selected in confirmation window
        close_error_click:      [int] Close message
        open_new_host:          [int]  Open form to submit new host
        submit_new_host:        [int] Submit new host
        comp_table:             [List] Information in comp_table
        action:                 [int] Action to confirm
        nickname:               [str] Host nickname
        hostname:               [str] Hostname
        privacy:                [bool] public or not public
        front_num_workers:      [int] Number of workers for frontend services
        front_num_cpus:         [int] Number of processors for frontend services
        front_list_gpus:        [str] List of GPUs for frontend services
        back_num_workers:       [int] Number of workers for backend services
        back_num_cpus:          [int] Number of processors for backend services
        back_list_gpus:         [str] List of GPUs for backend services
        query_name:             [str] Nickname of compute resource for query or new host
        query_hostname:         [str] Hostname of compute resource for query or new host
    Return:
        warning_body:           [str] Warning message body
        warning_open:           [bool] Open/close warning message
        msg_body:               [str] Pop-up messahe body
        msg_open:               [bool] Open pop-up message
        open_host_form:         [bool] Open new host form
        action:                 [int] Action to confirm
        nickname_out:           [str] Host nickname
        hostname_out:           [str] Hostname
    '''
    changed_id = [p['prop_id'] for p in dash.callback_context.triggered][0]
    print(changed_id)
    warning_body = dash.no_update
    warning_open = dash.no_update
    msg_body = dash.no_update
    msg_open = dash.no_update
    open_host_form = dash.no_update
    nickname_out = dash.no_update
    hostname_out = dash.no_update
    
    if 'reset-database' in changed_id:
        warning_body ='The workflow database will be deleted with this action. Do you want to continue?'
        warning_open = True
        action = 0

    if 'reset-host' in changed_id:
        if row is None:
            msg_body = "Select a host from the table"
            msg_open = True
        else:
            nickname = comp_table[row[0]]["nickname"]
            warning_body = f'The host ({nickname}) resources will be reset. Do you want to continue?'
            warning_open = True
            action = 1

    if 'delete-host' in changed_id:
        if row is None:
            msg_body = "Select a host from the table"
            msg_open = True
            action = -1
        else:
            nickname = comp_table[row[0]]["nickname"]
            warning_body = f'The host ({nickname}) will be deleted with this action. Do you want to continue?'
            warning_open = True
            action = 2

    if 'warning-body.cancel_n_clicks' in changed_id:
        warning_open = False
        action = -1

    if 'warning-body.submit_n_clicks' in changed_id:
        if action == 0:
            response = requests.delete(f'{COMP_URL}system/reset').status_code
        if action == 1:
            host_uid = comp_table[row[0]]["host_id"]
            response = requests.patch(f'{COMP_URL}host/{host_uid}/reset').status_code
        if action == 2:
            host_uid = comp_table[row[0]]["host_id"]
            response = requests.delete(f'{COMP_URL}host/{host_uid}/delete').status_code
        warning_open = False
        action = -1
        if response == 200:
            msg_body = 'The changes were completed succesfully. It can take some few seconds for the changes to be ' \
                       'reflected on the plot.'
        else:
            msg_body = f'Error {response}'
        msg_open = True
        print(f'variable {msg_open}')

    if 'open-new-host' in changed_id:
        nickname_out = query_name
        hostname_out = query_hostname
        open_host_form = True

    if 'submit-new-host' in changed_id:
        mlex_host = MlexHost(nickname[0], hostname[0], front_num_workers[0], front_num_cpus[0],
                             front_list_gpus[0], back_num_workers[0], back_num_cpus[0],
                             back_list_gpus[0])
        response = requests.post(f'{COMP_URL}hosts', json=mlex_host.__dict__)
        if response.status_code == 200:
            msg_body = 'The host has been sucessfully submitted!'
        else:
            msg_body = f'The host was not submitted. Error code: {response.status_code}'
        msg_open = True
        open_host_form = False

    if 'close-msg' in changed_id:
        msg_open = False
        action = -1

    return warning_body, warning_open, msg_body, msg_open, open_host_form, action, [nickname_out], [hostname_out]


if __name__ == "__main__":
    app.run_server(debug=True, host='0.0.0.0', port=8050)
