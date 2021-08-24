import pandas as pd
import datetime as dt


class PortfolioAnalyzer:
    """ class for analyzing financial data """
    def __init__(self, csv_path):
        self.df = pd.read_csv(csv_path)
        self.df['trddatetime'] = pd.TimedeltaIndex(self.df['trddatetime'], unit='d') + dt.datetime(1899, 12, 30)
        self.df['year'] = self.df['trddatetime'].dt.year
        self.df['month'] = self.df['trddatetime'].astype('period[M]')
        self.df['quarter'] = self.df['trddatetime'].astype('period[Q]')
        self.result = pd.DataFrame()
        self.period = None

    def cashflow_and_mean(self):
        """
        calculates the cashflow, mean, normalized cashflow for the client
        the numbers indicate requirements
        :return: None
        """
        df = self.df.groupby(['clientid', self.period, 'dealside']).agg(price=('price', 'sum'),
                                                                        dealsize_mean=(
                                                                            'dealsize', 'mean')).reset_index()
        seller_df = df[df['dealside'] == "S"].set_index(['clientid', self.period])
        buy_df = df[df['dealside'] == "B"].set_index(['clientid', self.period])
        self.result = seller_df[['price', 'dealsize_mean']].join(buy_df, rsuffix='_buy', lsuffix='_sell', how='outer')
        self.result = self.result.fillna(0)
        self.result['cashflow(3)'] = self.result['price_sell'] - self.result['price_buy']
        temp = self.result.reset_index().groupby(['clientid'])[['price_sell', 'price_buy']].sum()
        temp['total'] = temp['price_sell'] - temp['price_buy']
        self.result = self.result.join(temp['total'])
        self.result['total_cashflow_norm'] = self.result['cashflow(3)'] / self.result['total']

    def calculate_transactions(self):
        """
        calculates transactions and normalized transactions for the client
        :return: None
        """
        grp = self.df.groupby(['clientid', self.period])
        temp = grp['dealside'].value_counts().unstack()
        temp = temp.fillna(0)
        temp['total_trans'] = temp['B'] + temp['S']
        self.result = self.result.join(temp)
        temp = self.result[['B', 'S', 'total_trans']] / self.result[['B', 'S', 'total_trans']].sum()
        temp.columns = ['buy_norm(8)', 'sell_norm(9)', 'tot_trans_norm(10)']
        self.result = self.result.join(temp)

    def transform(self):
        """
        fixes data for the output format
        :return:
        """
        self.result.drop('total', axis=1)
        self.result = self.result.reset_index()
        self.result = self.result.rename(columns={
            'price_sell': 'price_sell(1)',
            'price_buy': 'price_buy(2)',
            'total_cashflow_norm': 'total_cashflow_norm(4)',
            'B': 'buy_trans(5)',
            'S': 'sell_trans(6)',
            'total_trans': 'total_trans(7)',
            'dealsize_mean_sell': 'mean_sell_size(11)',
            'dealsize_mean_buy': 'mean_buy_size(12)',
            self.period: 'time_period'

        })
        self.result = self.result[['clientid', 'time_period', 'price_sell(1)', 'price_buy(2)', 'cashflow(3)', 'total_cashflow_norm(4)',
                                   'buy_trans(5)', 'sell_trans(6)', 'total_trans(7)',
                                   'buy_norm(8)', 'sell_norm(9)', 'tot_trans_norm(10)',
                                   'mean_sell_size(11)', 'mean_sell_size(11)']]

    def execute(self, period):
        """
        main function which executes the analysis
        :param period: str: values are (month, year, quarter)
        :return: DataFrame: returns a dataframe of the analysis
        """
        self.period = period
        self.cashflow_and_mean()
        self.calculate_transactions()
        self.transform()
        return self.result


