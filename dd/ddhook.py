from airflow.hooks.dbapi_hook import DbApiHook


class DDHook(DbApiHook):
    conn_name_attr = 'profile'
    default_conn_name = 'default'
    supports_autocommit = True

    def __init__(self, profile):
        super(DDHook, self).__init__(profile=profile)
