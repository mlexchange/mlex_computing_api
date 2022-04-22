from dash import Dash, Output, Input, State, html, dcc, callback, MATCH
import uuid

class JobTableAIO(html.Div):

    class ids:
        datatable = lambda aio_id: {
            'component': 'DataTableAIO',
            'subcomponent': 'datatable',
            'aio_id': aio_id
        }
        store = lambda aio_id: {
            'component': 'DataTableAIO',
            'subcomponent': 'store',
            'aio_id': aio_id
        }
    ids = ids

    def __init__(self, df=None, aio_id=None, **datatable_props):
        """DataTableIO is an All-in-One component that is composed of a parent `html.Div`
        with a `dcc.Store` and a `dash_table.DataTable` as children.
        The dataframe filtering, paging, and sorting is performed in a built-in
        callback that uses Pandas.

        The DataFrame is stored in Redis as a Parquet file via the
        `redis_store` class. The `dcc.Store` contains the Redis key to the
        DataFrame and can be retrieved with `redis_store.get(store['df'])`
        in a separate callback.

        The underlying functions that filter, sort, and page the data are
        accessible via `filter_df`, `sort_df`, and `page_df` respectively.

        - `df` - A Pandas dataframe
        - `aio_id` - The All-in-One component ID used to generate the `dcc.Store` and `DataTable` components's dictionary IDs.
        - `**datatable_props` - Properties passed into the underlying `DataTable`
        """
        if aio_id is None:
            aio_id = str(uuid.uuid4())
            # Infer DataTable column types from the Pandas DataFrame
            columns = []
            columns_cast_to_string = []
            for c in df.columns:
                column = {'name': c, 'id': c}
                dtype = pd.api.types.infer_dtype(df[c])
                if dtype.startswith('mixed'):
                    columns_cast_to_string.append(c)
                    df[c] = df[c].astype(str)

                if pd.api.types.is_numeric_dtype(df[c]):
                    column['type'] = 'numeric'
                elif pd.api.types.is_string_dtype(df[c]):
                    column['type'] = 'text'
                elif pd.api.types.is_datetime64_any_dtype(df[c]):
                    column['type'] = 'datetime'
                else:
                    columns_cast_to_string.append(c)
                    df[c] = df[c].astype(str)
                    column['type'] = 'text'
                columns.append(column)

            if columns_cast_to_string:
                warnings.warn(
                    'Converted the following mixed-type columns to ' +
                    'strings so that they can be saved in Redis or JSON: ' +
                    f'{", ".join(columns_cast_to_string)}'
                )

            derived_kwargs = datatable_props.copy()

            # Store the DataFrame in Redis and the hash key in `dcc.Store`
            # Allow the user to pass in `df=` or `data=` as per `DataTable`.
            store_data = {}
            if df is None and 'data' in datatable_props:
                store_data['df'] = redis_store.save(
                    pd.DataFrame(datatable_props['data'])
                )
            elif df is not None and not 'data' in datatable_props:
                store_data['df'] = redis_store.save(df)
            elif df is not None and 'data' in datatable_props:
                raise Exception('The `df` argument cannot be supplied with the data argument - it\'s ambiguous.')
            else:
                raise Exception('No data supplied. Pass in a dataframe as `df=` or a list of dictionaries as `data=`')

            # Allow the user to pass in their own columns, otherwise define our own.
            if df is not None:
                if 'columns' not in datatable_props:
                    derived_kwargs['columns'] = columns

            # Allow the user to override these properties, otherwise provide defaults
            derived_kwargs['page_current'] = derived_kwargs.get('page_current', 0)
            derived_kwargs['page_size'] = derived_kwargs.get('page_size', 10)
            derived_kwargs['page_action'] = derived_kwargs.get('page_action', 'custom')
            derived_kwargs['filter_action'] = derived_kwargs.get('filter_action', 'custom')
            derived_kwargs['filter_query'] = derived_kwargs.get('filter_query', '')
            derived_kwargs['sort_action'] = derived_kwargs.get('sort_action', 'custom')
            derived_kwargs['sort_mode'] = derived_kwargs.get('sort_mode', 'multi')
            derived_kwargs['sort_by'] = derived_kwargs.get('sort_by', [])

            super().__init__([
                dcc.Store(data=store_data, id=self.ids.store(aio_id)),
                dash_table.DataTable(id=self.ids.datatable(aio_id), **derived_kwargs)
            ])
        
        