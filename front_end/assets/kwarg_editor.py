import re
from typing import Callable
# noinspection PyUnresolvedReferences
from inspect import signature, _empty

import dash
import dash_html_components as html
import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash_daq as daq

import base64
import PIL.Image
import io
import plotly.express as px

from dash.dependencies import Input, ALL, Output, State

from targeted_callbacks import targeted_callback

# Procedural dash form generation

"""
{'name', 'title', 'value', 'type', 
"""


class SimpleItem(dbc.FormGroup):     
    def __init__(self,
                 name,
                 base_id,
                 title=None,
                 param_key=None,
                 type='number',
                 debounce=True,
                 **kwargs):
        self.name = name

        self.label = dbc.Label(title or name)
        self.input = dbc.Input(type=type,
                               debounce=debounce,
                               id={**base_id,
                                   'name': name,
                                   'param_key': param_key},
                               **kwargs)

        super(SimpleItem, self).__init__(children=[self.label, self.input])


class FloatItem(SimpleItem):
    pass


class IntItem(SimpleItem):
    def __init__(self, *args, **kwargs):
        if 'min' not in kwargs:
            kwargs['min'] = -9007199254740991  
        super(IntItem, self).__init__(*args, step=1, **kwargs)


class StrItem(SimpleItem):
    def __init__(self, *args, **kwargs):
        super(StrItem, self).__init__(*args, type='text', **kwargs)
        
        
class SliderItem(dbc.FormGroup):
    def __init__(self,
                 name,       
                 base_id,   
                 title=None,
                 param_key=None,
                 debounce=True,
                 visible=True,
                 **kwargs):

        self.label = dbc.Label(title or name)
        self.input = dcc.Slider(id={**base_id,
                                    'name': name,
                                    'param_key': param_key,
                                    'layer': 'input'},
                                    tooltip={"placement": "bottom", "always_visible": True},
                                    **kwargs)

        style = {}
        if not visible:
            style['display'] = 'none'

        super(SliderItem, self).__init__(id={**base_id,
                                             'name': name,
                                             'param_key': param_key,
                                             'layer': 'form_group'},
                                         children=[self.label, self.input],
                                         style=style)


class DropdownItem(dbc.FormGroup):
    def __init__(self,
                 name,       
                 base_id,  
                 title=None,
                 param_key=None,
                 debounce=True,
                 visible=True,
                 **kwargs):

        self.label = dbc.Label(title or name)
        self.input = dcc.Dropdown(id={**base_id,
                                    'name': name,
                                    'param_key': param_key,
                                    'layer': 'input'},
                                **kwargs)

        style = {}
        if not visible:
            style['display'] = 'none'

        super(DropdownItem, self).__init__(id={**base_id,
                                                 'name': name,
                                                 'param_key': param_key,
                                                 'layer': 'form_group'},
                                             children=[self.label, self.input],
                                             style=style)


class RadioItem(dbc.FormGroup):
    def __init__(self,
                 name,
                 base_id,
                 title=None,
                 param_key=None,
                 visible=True,
                 **kwargs):

        self.label = dbc.Label(title or name)
        self.input = dbc.RadioItems(id={**base_id,
                                        'name': name,
                                        'param_key': param_key,
                                        'layer': 'input'},
                                    **kwargs)

        style = {}
        if not visible:
            style['display'] = 'none'

        super(RadioItem, self).__init__(id={**base_id,
                                               'name': name,
                                               'param_key': param_key,
                                               'layer': 'form_group'},
                                           children=[self.label, self.input],
                                           style=style)


class BoolItem(dbc.FormGroup):
    def __init__(self,
                 name,
                 base_id,
                 title=None,
                 param_key=None,
                 visible=True,
                 **kwargs):

        self.label = dbc.Label(title or name)
        self.input = daq.ToggleSwitch(id={**base_id,
                                          'name': name,
                                          'param_key': param_key,
                                          'layer': 'input'},
                                      **kwargs)
        self.output_label = dbc.Label('False/True')

        style = {}
        if not visible:
            style['display'] = 'none'

        super(BoolItem, self).__init__(id={**base_id,
                                           'name': name,
                                           'param_key': param_key,
                                           'layer': 'form_group'},
                                       children=[self.label, self.input, self.output_label],
                                       style=style)


class GraphItem(dbc.FormGroup):
    def __init__(self,
                 name,
                 base_id,
                 title=None,
                 param_key=None,
                 visible=True,
                 figure = None,
                 **kwargs):

        self.name = name
        self.label = dbc.Label(title or name)
        self.input_graph = dcc.Graph(id={**base_id,
                                    'name': self.name,
                                    'param_key': param_key,
                                    'layer': 'input'},
                                **kwargs)
                                
        self.input_upload = dcc.Upload(id={**base_id,
                                    'name': self.name+'_upload',
                                    'param_key': param_key,
                                    'layer': 'input'},
                                    children=html.Div([
                                        'Drag and Drop or ',
                                        html.A('Select Files')
                                    ]),
                                    style={
                                        'width': '95%',
                                        'height': '60px',
                                        'lineHeight': '60px',
                                        'borderWidth': '1px',
                                        'borderStyle': 'dashed',
                                        'borderRadius': '5px',
                                        'textAlign': 'center',
                                        'margin': '10px'
                        },
                        multiple = False)

        style = {}
        if not visible:
            style['display'] = 'none'

        super(GraphItem, self).__init__(id={**base_id,
                                           'name': self.name,
                                           'param_key': param_key,
                                           'layer': 'form_group'},
                                       children=[self.label, self.input_upload, self.input_graph],
                                       style=style)
    def return_upload(self, contents):
        img_bytes = base64.b64decode(contents.split(",")[1])
        img = PIL.Image.open(io.BytesIO(img_bytes))
        fig = px.imshow(img, binary_string=True)
        return fig 
        
    def init_callbacks(self, app):
        targeted_callback(self.return_upload,
                          Input({**self.id,
                                 'name': self.name+'_upload',
                                 'layer': 'input'},
                                'contents'),
                          Output({**self.id,
                                 'name': self.name,
                                 'layer': 'input'}, 'figure'),
                          State({**self.id,
                                 'name': self.name+'_upload',
                                 'layer': 'input'}, 'last_modified'),
                          State({**self.id,
                                 'name': self.name+'_upload',
                                 'layer': 'input'}, 'filename'),
                          app=app)



class ParameterEditor(dbc.Form):

    type_map = {float: FloatItem,
                int: IntItem,
                str: StrItem,
                }

    def __init__(self, _id, parameters, **kwargs):
        self._parameters = parameters

        super(ParameterEditor, self).__init__(id=_id, children=[], className='kwarg-editor', **kwargs)
        self.children = self.build_children()

    def init_callbacks(self, app):
        targeted_callback(self.stash_value,
                          Input({**self.id,
                                 'name': ALL},
                                'value'),
                          Output(self.id, 'n_submit'),
                          State(self.id, 'n_submit'),
                          app=app)
        for child in self.children:
            if hasattr(child,"init_callbacks"):
                child.init_callbacks(app)
     
    
    def stash_value(self, value):  # update changed value 
        # find the changed item name from regex
        r = '(?<=\"name\"\:\")[\w\-_]+(?=\")'
        matches = re.findall(r, dash.callback_context.triggered[0]['prop_id'])

        if not matches:
            raise LookupError('Could not find changed item name. Check that all parameter names use simple chars (\\w)')

        name = matches[0]
        self.parameters[name]['value'] = value

        print(self.values)

        return (next(iter(dash.callback_context.states.values())) or 0) + 1   

    @property
    def values(self):
        return {param['name']: param.get('value', None) for param in self._parameters} 

    @property
    def parameters(self):
        return {param['name']: param for param in self._parameters}

    def _determine_type(self, parameter_dict):
        if 'type' in parameter_dict:
            if parameter_dict['type'] in self.type_map:
                return parameter_dict['type']
            elif parameter_dict['type'].__name__ in self.type_map:
                return parameter_dict['type'].__name__
        elif type(parameter_dict['value']) in self.type_map:
            return type(parameter_dict['value'])
        raise TypeError(f'No item type could be determined for this parameter: {parameter_dict}')

    def build_children(self, values=None):
        children = []
        for parameter_dict in self._parameters:
            parameter_dict = parameter_dict.copy()
            if values and parameter_dict['name'] in values:
                parameter_dict['value'] = values[parameter_dict['name']]
            type = self._determine_type(parameter_dict)
            parameter_dict.pop('type', None)
            item = self.type_map[type](**parameter_dict, base_id=self.id) 
            children.append(item)

        return children
        

class JSONParameterEditor(ParameterEditor):
    type_map = {'float': FloatItem,
                'int': IntItem,
                'str': StrItem,
                'slider': SliderItem,
                'dropdown': DropdownItem,
                'radio': RadioItem,
                'bool': BoolItem,
                'graph': GraphItem,
                }

    def __init__(self, _id, json_blob, **kwargs):
        super(ParameterEditor, self).__init__(id=_id, children=[], className='kwarg-editor', **kwargs)
        self._json_blob = json_blob
        self.children = self.build_children()

    def build_children(self, values=None):
        children = []
        for json_record in self._json_blob:
            ...
            # build a parameter dict from self.json_blob
            ...
            type = json_record.get('type', self._determine_type(json_record))   
            json_record = json_record.copy()
            if values and json_record['name'] in values:
                json_record['value'] = values[json_record['name']]
            json_record.pop('type', None)
            item = self.type_map[type](**json_record, base_id=self.id)
            children.append(item)

        return children


class KwargsEditor(ParameterEditor):
    def __init__(self, instance_index, func: Callable, **kwargs):
        self.func = func
        self._instance_index = instance_index

        parameters = [{'name': name, 'value': param.default} for name, param in signature(func).parameters.items()
                      if param.default is not _empty]

        super(KwargsEditor, self).__init__(dict(index=instance_index, type='kwargs-editor'), parameters=parameters, **kwargs)

    def new_record(self):
        return {name: p.default for name, p in signature(self.func).parameters.items() if p.default is not _empty}
