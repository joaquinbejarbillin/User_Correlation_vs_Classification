
import pandas as pd
from sqlalchemy import create_engine

import matplotlib.pyplot as plt
#from pandas.tools.plotting import scatter_matrix
from pandas.plotting import scatter_matrix

import collections
import numpy as np

import dill
import pymongo
from pymongo import MongoClient


class DB():


    def __init__(self, user=None, password=None, host=None, db=None):
        __host = '10.0.5.225'
        __db = 'billin'
        __user = 'postgres'
        __password = 'ppoSgre123'

        if user is None:
            self.user = __user
        else:
            self.user = user

        if password is None:
            self.password = __password
        else:
            self.password = password

        if host is None:
            self.host = __host
        else:
            self.host = host

        if db is None:
            self.db = __db
        else:
            self.db = db

        self.engine = create_engine('postgresql://{}:{}@{}:5432/{}'.format(self.user, self.password, self.host, self.db))


    def gettable(self, table):
        #contacts = pd.read_sql_query('select * from "contacts"',con=engine)
        #users = pd.read_sql_query('select * from "users"',con=engine)
        #sessions = pd.read_sql_query('select * from "sessions"',con=engine)
        #premiums = pd.read_sql_query('select * from "premiums"',con=engine)
        #gocardlesses = pd.read_sql_query('select * from "gocardlesses"',con=engine)
        #campaigns = pd.read_sql_query('select * from "campaigns"',con=engine)
        #campaign_details = pd.read_sql_query('select * from "campaign-details"',con=engine)
        #businessesUsers = pd.read_sql_query('select * from "businessesUsers"',con=engine)
        #businesses = pd.read_sql_query('select * from "businesses"',con=engine)
        #businessConstacts = pd.read_sql_query('select * from "businessContacts"',con=engine)
        #bankAccounts = pd.read_sql_query('select * from "bankAccounts"',con=engine)
        #addresses = pd.read_sql_query('select * from "addresses"',con=engine)

        return pd.read_sql_query('select * from "{}"'.format(table),con=self.engine)


def days_ago(__serie):
    today = int(pd.datetime.now().timestamp())
    name = __serie.name

    __serie = (today - pd.to_datetime(__serie, dayfirst=True).astype(int) // 10 ** 9) // 86400


    __serie.name = '{}_days_ago'.format(name)

    return __serie


def last_months(__df, months=6):
    business = __df.business.unique()
    to_serie = dict()

    for b in business:
        bdf = __df[__df.business == b]
        periods = -months
        temp = {}
        for i in pd.date_range(end=pd.datetime.now(), periods=months, freq='M'):
            date = pd.to_datetime('1/{}/{}'.format(i.month, i.year), dayfirst=True)
            mes = bdf[bdf['created_at'].between(date, pd.to_datetime(pd.date_range(start=date, periods=1, freq='M').item()))]
            temp[periods] = mes[mes.business == b].shape[0]
            periods += 1

        to_serie[b] = temp

    results = pd.DataFrame.from_dict(to_serie, orient='index')
    results['mean'] = results.mean(axis=1)
    return results


def get_invoices(user=None, password=None, host=None, db=None):
    '''
    get_invoices('nodejs','TresTRISTESTIGRESComianTrigoEnUnTrigal', 'production-shard-00-00-qlmse.mongodb.net', 'billin_prod')

    :param user:
    :param password:
    :param host:
    :param db:
    :return:
    '''
    client = MongoClient('mongodb://{}:{}@{}:27017'.format(user, password, host), ssl=True)

    # Get the sampleDB database
    db = client[db]

    invoices = db.get_collection('invoices')

    j = invoices.find({}, {'_id': 0, 'created_at': 1, 'business.id': 1})

    df = pd.DataFrame(list(j))

    df['business'] = df.apply(lambda row: row.business['id'] if row.business != {} else None, axis=1)

    return df